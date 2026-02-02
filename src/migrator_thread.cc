#include "migrator_thread.h"
#include "const.h"
#include "event_builder.h"
#include "event_filter.h"

#include <unistd.h>

#include <glog/logging.h>
#include <algorithm>
#include <functional>
#include <map>
#include <type_traits>
#include <string_view>
#include <vector>

#include "net/include/redis_cli.h"
#include "storage/storage.h"

const int64_t MAX_BATCH_NUM = 30000;

namespace {

int64_t ScanBatchCount() {
  int64_t batch_count = g_conf.sync_batch_num * 10;
  if (MAX_BATCH_NUM < batch_count) {
    if (g_conf.sync_batch_num < MAX_BATCH_NUM) {
      batch_count = MAX_BATCH_NUM;
    } else {
      batch_count = g_conf.sync_batch_num * 2;
    }
  }
  return batch_count;
}

bool ShouldSkipKey(const std::string& key) {
  return key.compare(0, SlotKeyPrefix.size(), SlotKeyPrefix) == 0;
}

template <typename T, typename = void>
struct HasTTLOneArg : std::false_type {};

template <typename T>
struct HasTTLOneArg<T, std::void_t<decltype(static_cast<int64_t (T::*)(const storage::Slice&)>(
                               &T::TTL))>> : std::true_type {};

template <typename T, typename = void>
struct HasTTLTwoArg : std::false_type {};

template <typename T>
struct HasTTLTwoArg<
    T,
    std::void_t<decltype(static_cast<std::map<storage::DataType, int64_t> (T::*)(
                               const storage::Slice&, std::map<storage::DataType, storage::Status>*)>(
        &T::TTL))>> : std::true_type {};

template <typename StorageT>
int64_t GetTTLCompat(StorageT* db, const std::string& key, storage::DataType type) {
  if constexpr (HasTTLTwoArg<StorageT>::value) {
    std::map<storage::DataType, storage::Status> type_status;
    std::map<storage::DataType, int64_t> ttl_map = db->TTL(storage::Slice(key), &type_status);
    auto it = ttl_map.find(type);
    if (it != ttl_map.end()) {
      return it->second;
    }
    return -1;
  }
  if constexpr (HasTTLOneArg<StorageT>::value) {
    return db->TTL(storage::Slice(key));
  }
  static_assert(HasTTLOneArg<StorageT>::value || HasTTLTwoArg<StorageT>::value,
                "storage::Storage::TTL signature not detected");
  return -1;
}

std::string DataTypeName(int type) {
  switch (static_cast<int>(type)) {
    case static_cast<int>(storage::DataType::kStrings):
      return "string";
    case static_cast<int>(storage::DataType::kLists):
      return "list";
    case static_cast<int>(storage::DataType::kHashes):
      return "hash";
    case static_cast<int>(storage::DataType::kSets):
      return "set";
    case static_cast<int>(storage::DataType::kZSets):
      return "zset";
    default:
      return "unknown";
  }
}

bool ShouldSendSnapshotEvent(const net::RedisCmdArgsType& argv,
                             const std::string& data_type,
                             const std::string& key) {
  const EventFilter* filter = g_conf.event_filter.get();
  if (!filter) {
    return true;
  }
  std::string_view action;
  if (!argv.empty()) {
    action = argv[0];
  }
  return filter->ShouldSend(key, data_type, action);
}

bool ShouldSendSnapshotKeyAction(const std::string& data_type,
                                 const std::string& key,
                                 std::string_view action) {
  const EventFilter* filter = g_conf.event_filter.get();
  if (!filter) {
    return true;
  }
  return filter->ShouldSend(key, data_type, action);
}

KafkaRecord MakeSnapshotRecord(const net::RedisCmdArgsType& argv,
                               const std::string& data_type,
                               const std::string& key,
                               const std::string& raw_resp) {
  KafkaRecord record;
  if (g_conf.kafka_stream_mode == "single") {
    record.topic = g_conf.kafka_topic_single;
  } else {
    record.topic = g_conf.kafka_topic_snapshot;
  }
  record.key = BuildPartitionKey(g_conf.db_name, data_type, key);
  record.payload = BuildSnapshotEventJson(argv, g_conf.db_name, data_type, g_conf.source_id, raw_resp, key);
  record.has_checkpoint = false;
  return record;
}

}  // namespace

MigratorThread::~MigratorThread() = default;

void MigratorThread::MigrateStringsDB() {
  auto* db = db_;
  const std::string data_type = DataTypeName(type_);
  std::string pattern("*");
  int64_t cursor = 0;
  int64_t batch_count = ScanBatchCount();

  do {
    std::vector<std::string> keys;
    cursor = db->Scan(storage::DataType::kStrings, cursor, pattern, batch_count, &keys);

    for (const auto& k : keys) {
      if (should_exit_) {
        break;
      }
      if (ShouldSkipKey(k)) {
        continue;
      }
      PlusKeysScanned();
      if (!ShouldSendSnapshotKeyAction(data_type, k, "set")) {
        continue;
      }

      std::string value;
      storage::Status s = db->Get(k, &value);
      if (!s.ok()) {
        LOG(WARNING) << "db->Get(key:" << k << ") = " << s.ToString();
        continue;
      }

      net::RedisCmdArgsType argv;
      std::string cmd;

      argv.push_back("SET");
      argv.push_back(k);
      argv.push_back(value);
      int64_t ttl = GetTTLCompat(db, k, storage::DataType::kStrings);
      if (ttl > 0) {
        argv.push_back("EX");
        argv.push_back(std::to_string(ttl));
      }

      if (!ShouldSendSnapshotEvent(argv, data_type, k)) {
        continue;
      }
      net::SerializeRedisCommand(argv, &cmd);
      KafkaRecord record = MakeSnapshotRecord(argv, data_type, k, cmd);
      if (IsPayloadTooLarge(record) &&
          g_conf.snapshot_oversize_string_policy == SnapshotOversizeStringPolicy::kSkip) {
        LOG(WARNING) << "snapshot string payload too large, skip key=" << k
                     << " payload_size=" << record.payload.size();
        continue;
      }
      PlusNum();
      DispatchRecord(record);
    }
  } while (cursor != 0 && !should_exit_);
}

void MigratorThread::MigrateListsDB() {
  auto* db = db_;
  const std::string data_type = DataTypeName(type_);
  std::string pattern("*");
  int64_t cursor = 0;
  int64_t batch_count = ScanBatchCount();

  do {
    std::vector<std::string> keys;
    cursor = db->Scan(storage::DataType::kLists, cursor, pattern, batch_count, &keys);

    for (const auto& k : keys) {
      if (should_exit_) {
        break;
      }
      if (ShouldSkipKey(k)) {
        continue;
      }
      PlusKeysScanned();
      const bool allow_rpush = ShouldSendSnapshotKeyAction(data_type, k, "rpush");
      const bool allow_expire = ShouldSendSnapshotKeyAction(data_type, k, "expire");
      if (!allow_rpush && !allow_expire) {
        continue;
      }

      int64_t pos = 0;
      std::vector<std::string> list;
      if (allow_rpush) {
        storage::Status s = db->LRange(k, pos, pos + g_conf.sync_batch_num - 1, &list);
        if (!s.ok()) {
          LOG(WARNING) << "db->LRange(key:" << k << ", pos:" << pos << ", batch size:" << g_conf.sync_batch_num
                       << ") = " << s.ToString();
          continue;
        }

        while (s.ok() && !should_exit_ && !list.empty()) {
          net::RedisCmdArgsType argv;
          std::string cmd;

          argv.push_back("RPUSH");
          argv.push_back(k);
          for (const auto& e : list) {
            argv.push_back(e);
          }

          if (!ShouldSendSnapshotEvent(argv, data_type, k)) {
            break;
          }
          net::SerializeRedisCommand(argv, &cmd);
          KafkaRecord record = MakeSnapshotRecord(argv, data_type, k, cmd);
          if (IsPayloadTooLarge(record) && g_conf.snapshot_oversize_list_tail_max_items > 0) {
            LOG(WARNING) << "snapshot list payload too large, fallback to last "
                         << g_conf.snapshot_oversize_list_tail_max_items << " entries, key=" << k;
            SendListTailSnapshot(k, data_type);
            break;
          }
          PlusNum();
          DispatchRecord(record);

          pos += g_conf.sync_batch_num;
          list.clear();
          s = db->LRange(k, pos, pos + g_conf.sync_batch_num - 1, &list);
          if (!s.ok()) {
            LOG(WARNING) << "db->LRange(key:" << k << ", pos:" << pos << ", batch size:" << g_conf.sync_batch_num
                         << ") = " << s.ToString();
          }
        }
      }

      if (allow_expire) {
        int64_t ttl = GetTTLCompat(db, k, storage::DataType::kLists);
        if (ttl > 0) {
          net::RedisCmdArgsType argv;
          std::string cmd;

          argv.push_back("EXPIRE");
          argv.push_back(k);
          argv.push_back(std::to_string(ttl));
          if (!ShouldSendSnapshotEvent(argv, data_type, k)) {
            continue;
          }
          net::SerializeRedisCommand(argv, &cmd);
          KafkaRecord record = MakeSnapshotRecord(argv, data_type, k, cmd);
          PlusNum();
          DispatchRecord(record);
        }
      }
    }
  } while (cursor != 0 && !should_exit_);
}

bool MigratorThread::IsPayloadTooLarge(const KafkaRecord& record) const {
  if (g_conf.kafka_message_max_bytes <= 0) {
    return false;
  }
  return record.payload.size() > static_cast<size_t>(g_conf.kafka_message_max_bytes);
}

bool MigratorThread::SendListTailSnapshot(const std::string& key, const std::string& data_type) {
  if (!db_) {
    return false;
  }
  if (g_conf.snapshot_oversize_list_tail_max_items == 0) {
    return false;
  }
  const size_t tail_max = g_conf.snapshot_oversize_list_tail_max_items;
  std::vector<std::string> tail;
  storage::Status s = db_->LRange(key, -static_cast<int64_t>(tail_max), -1, &tail);
  if (!s.ok()) {
    LOG(WARNING) << "db->LRange(key:" << key << ", pos:-" << tail_max << ", batch size:" << tail_max
                 << ") = " << s.ToString();
    return false;
  }
  if (tail.empty()) {
    return true;
  }

  size_t idx = 0;
  while (idx < tail.size() && !should_exit_) {
    size_t remaining = tail.size() - idx;
    size_t batch = std::min(tail_max, remaining);
    while (batch > 0 && !should_exit_) {
      net::RedisCmdArgsType argv;
      std::string cmd;
      argv.push_back("RPUSH");
      argv.push_back(key);
      for (size_t i = 0; i < batch; ++i) {
        argv.push_back(tail[idx + i]);
      }
      net::SerializeRedisCommand(argv, &cmd);
      KafkaRecord record = MakeSnapshotRecord(argv, data_type, key, cmd);
      if (IsPayloadTooLarge(record)) {
        if (batch == 1) {
          LOG(WARNING) << "snapshot list tail entry too large, drop entry key=" << key
                       << " payload_size=" << record.payload.size();
          break;
        }
        batch = (batch + 1) / 2;
        continue;
      }
      PlusNum();
      DispatchRecord(record);
      break;
    }
    idx += std::max<size_t>(1, batch);
  }
  return true;
}

void MigratorThread::MigrateHashesDB() {
  auto* db = db_;
  const std::string data_type = DataTypeName(type_);
  std::string pattern("*");
  int64_t cursor = 0;
  int64_t batch_count = ScanBatchCount();

  do {
    std::vector<std::string> keys;
    cursor = db->Scan(storage::DataType::kHashes, cursor, pattern, batch_count, &keys);

    for (const auto& k : keys) {
      if (should_exit_) {
        break;
      }
      if (ShouldSkipKey(k)) {
        continue;
      }
      PlusKeysScanned();
      const bool allow_hmset = ShouldSendSnapshotKeyAction(data_type, k, "hmset");
      const bool allow_expire = ShouldSendSnapshotKeyAction(data_type, k, "expire");
      if (!allow_hmset && !allow_expire) {
        continue;
      }

      if (allow_hmset) {
        std::vector<storage::FieldValue> fvs;
        storage::Status s = db->HGetall(k, &fvs);
        if (!s.ok()) {
          LOG(WARNING) << "db->HGetall(key:" << k << ") = " << s.ToString();
          continue;
        }
        size_t index = 0;
        while (!should_exit_ && index < fvs.size()) {
          size_t batch = std::min(static_cast<size_t>(g_conf.sync_batch_num), fvs.size() - index);
          size_t used = batch;
          bool drop_key = false;
          while (used > 0 && !should_exit_) {
            net::RedisCmdArgsType argv;
            std::string cmd;

            argv.push_back("HMSET");
            argv.push_back(k);
            for (size_t i = 0; i < used; ++i) {
              const auto& fv = fvs[index + i];
              argv.push_back(fv.field);
              argv.push_back(fv.value);
            }

            if (!ShouldSendSnapshotEvent(argv, data_type, k)) {
              drop_key = true;
              break;
            }
            net::SerializeRedisCommand(argv, &cmd);
            KafkaRecord record = MakeSnapshotRecord(argv, data_type, k, cmd);
            if (g_conf.snapshot_oversize_shrink_batch && IsPayloadTooLarge(record)) {
              if (used == 1) {
                LOG(WARNING) << "snapshot hash entry too large, drop field key=" << k
                             << " payload_size=" << record.payload.size();
                break;
              }
              used = (used + 1) / 2;
              continue;
            }
            PlusNum();
            DispatchRecord(record);
            break;
          }
          if (drop_key) {
            break;
          }
          index += std::max<size_t>(1, used);
        }
      }

      if (allow_expire) {
        int64_t ttl = GetTTLCompat(db, k, storage::DataType::kHashes);
        if (ttl > 0) {
          net::RedisCmdArgsType argv;
          std::string cmd;

          argv.push_back("EXPIRE");
          argv.push_back(k);
          argv.push_back(std::to_string(ttl));
          if (!ShouldSendSnapshotEvent(argv, data_type, k)) {
            continue;
          }
          net::SerializeRedisCommand(argv, &cmd);
          KafkaRecord record = MakeSnapshotRecord(argv, data_type, k, cmd);
          PlusNum();
          DispatchRecord(record);
        }
      }
    }
  } while (cursor != 0 && !should_exit_);
}

void MigratorThread::MigrateSetsDB() {
  auto* db = db_;
  const std::string data_type = DataTypeName(type_);
  std::string pattern("*");
  int64_t cursor = 0;
  int64_t batch_count = ScanBatchCount();

  do {
    std::vector<std::string> keys;
    cursor = db->Scan(storage::DataType::kSets, cursor, pattern, batch_count, &keys);

    for (const auto& k : keys) {
      if (should_exit_) {
        break;
      }
      if (ShouldSkipKey(k)) {
        continue;
      }
      PlusKeysScanned();
      const bool allow_sadd = ShouldSendSnapshotKeyAction(data_type, k, "sadd");
      const bool allow_expire = ShouldSendSnapshotKeyAction(data_type, k, "expire");
      if (!allow_sadd && !allow_expire) {
        continue;
      }

      if (allow_sadd) {
        std::vector<std::string> members;
        storage::Status s = db->SMembers(k, &members);
        if (!s.ok()) {
          LOG(WARNING) << "db->SMembers(key:" << k << ") = " << s.ToString();
          continue;
        }
        size_t index = 0;
        while (!should_exit_ && index < members.size()) {
          size_t batch = std::min(static_cast<size_t>(g_conf.sync_batch_num), members.size() - index);
          size_t used = batch;
          bool drop_key = false;
          while (used > 0 && !should_exit_) {
            std::string cmd;
            net::RedisCmdArgsType argv;

            argv.push_back("SADD");
            argv.push_back(k);
            for (size_t i = 0; i < used; ++i) {
              argv.push_back(members[index + i]);
            }

            if (!ShouldSendSnapshotEvent(argv, data_type, k)) {
              drop_key = true;
              break;
            }
            net::SerializeRedisCommand(argv, &cmd);
            KafkaRecord record = MakeSnapshotRecord(argv, data_type, k, cmd);
            if (g_conf.snapshot_oversize_shrink_batch && IsPayloadTooLarge(record)) {
              if (used == 1) {
                LOG(WARNING) << "snapshot set entry too large, drop member key=" << k
                             << " payload_size=" << record.payload.size();
                break;
              }
              used = (used + 1) / 2;
              continue;
            }
            PlusNum();
            DispatchRecord(record);
            break;
          }
          if (drop_key) {
            break;
          }
          index += std::max<size_t>(1, used);
        }
      }

      if (allow_expire) {
        int64_t ttl = GetTTLCompat(db, k, storage::DataType::kSets);
        if (ttl > 0) {
          net::RedisCmdArgsType argv;
          std::string cmd;

          argv.push_back("EXPIRE");
          argv.push_back(k);
          argv.push_back(std::to_string(ttl));
          if (!ShouldSendSnapshotEvent(argv, data_type, k)) {
            continue;
          }
          net::SerializeRedisCommand(argv, &cmd);
          KafkaRecord record = MakeSnapshotRecord(argv, data_type, k, cmd);
          PlusNum();
          DispatchRecord(record);
        }
      }
    }
  } while (cursor != 0 && !should_exit_);
}

void MigratorThread::MigrateZsetsDB() {
  auto* db = db_;
  const std::string data_type = DataTypeName(type_);
  std::string pattern("*");
  int64_t cursor = 0;
  int64_t batch_count = ScanBatchCount();

  do {
    std::vector<std::string> keys;
    cursor = db->Scan(storage::DataType::kZSets, cursor, pattern, batch_count, &keys);

    for (const auto& k : keys) {
      if (should_exit_) {
        break;
      }
      if (ShouldSkipKey(k)) {
        continue;
      }
      PlusKeysScanned();
      const bool allow_zadd = ShouldSendSnapshotKeyAction(data_type, k, "zadd");
      const bool allow_expire = ShouldSendSnapshotKeyAction(data_type, k, "expire");
      if (!allow_zadd && !allow_expire) {
        continue;
      }

      int64_t pos = 0;
      std::vector<storage::ScoreMember> score_members;
      if (allow_zadd) {
        storage::Status s = db->ZRange(k, static_cast<int32_t>(pos),
                                       static_cast<int32_t>(pos + g_conf.sync_batch_num - 1), &score_members);
        if (!s.ok()) {
          LOG(WARNING) << "db->ZRange(key:" << k << ", pos:" << pos << ", batch size:" << g_conf.sync_batch_num
                       << ") = " << s.ToString();
          continue;
        }

        while (s.ok() && !should_exit_ && !score_members.empty()) {
          size_t index = 0;
          while (!should_exit_ && index < score_members.size()) {
            size_t batch = std::min(static_cast<size_t>(g_conf.sync_batch_num), score_members.size() - index);
            size_t used = batch;
            bool drop_key = false;
            while (used > 0 && !should_exit_) {
              net::RedisCmdArgsType argv;
              std::string cmd;

              argv.push_back("ZADD");
              argv.push_back(k);

              for (size_t i = 0; i < used; ++i) {
                const auto& sm = score_members[index + i];
                argv.push_back(std::to_string(sm.score));
                argv.push_back(sm.member);
              }

              if (!ShouldSendSnapshotEvent(argv, data_type, k)) {
                drop_key = true;
                break;
              }
              net::SerializeRedisCommand(argv, &cmd);
              KafkaRecord record = MakeSnapshotRecord(argv, data_type, k, cmd);
              if (g_conf.snapshot_oversize_shrink_batch && IsPayloadTooLarge(record)) {
                if (used == 1) {
                  LOG(WARNING) << "snapshot zset entry too large, drop member key=" << k
                               << " payload_size=" << record.payload.size();
                  break;
                }
                used = (used + 1) / 2;
                continue;
              }
              PlusNum();
              DispatchRecord(record);
              break;
            }
            if (drop_key) {
              break;
            }
            index += std::max<size_t>(1, used);
          }
          pos += g_conf.sync_batch_num;
          score_members.clear();
          s = db->ZRange(k, static_cast<int32_t>(pos),
                         static_cast<int32_t>(pos + g_conf.sync_batch_num - 1), &score_members);
          if (!s.ok()) {
            LOG(WARNING) << "db->ZRange(key:" << k << ", pos:" << pos << ", batch size:" << g_conf.sync_batch_num
                         << ") = " << s.ToString();
          }
        }
      }

      if (allow_expire) {
        int64_t ttl = GetTTLCompat(db, k, storage::DataType::kZSets);
        if (ttl > 0) {
          net::RedisCmdArgsType argv;
          std::string cmd;

          argv.push_back("EXPIRE");
          argv.push_back(k);
          argv.push_back(std::to_string(ttl));
          if (!ShouldSendSnapshotEvent(argv, data_type, k)) {
            continue;
          }
          net::SerializeRedisCommand(argv, &cmd);
          KafkaRecord record = MakeSnapshotRecord(argv, data_type, k, cmd);
          PlusNum();
          DispatchRecord(record);
        }
      }
    }
  } while (cursor != 0 && !should_exit_);
}

void MigratorThread::MigrateDB() {
  switch (static_cast<int>(type_)) {
    case static_cast<int>(storage::DataType::kStrings): {
      MigrateStringsDB();
      break;
    }

    case static_cast<int>(storage::DataType::kLists): {
      MigrateListsDB();
      break;
    }

    case static_cast<int>(storage::DataType::kHashes): {
      MigrateHashesDB();
      break;
    }

    case static_cast<int>(storage::DataType::kSets): {
      MigrateSetsDB();
      break;
    }

    case static_cast<int>(storage::DataType::kZSets): {
      MigrateZsetsDB();
      break;
    }

    default: {
      LOG(WARNING) << "illegal db type " << type_;
      break;
    }
  }
}

void MigratorThread::DispatchRecord(const KafkaRecord& record) {
  thread_index_ = (thread_index_ + 1) % thread_num_;
  size_t idx = thread_index_;
  if (!record.key.empty()) {
    idx = std::hash<std::string>()(record.key) % thread_num_;
  }
  (*senders_)[idx]->Enqueue(record);
}

void* MigratorThread::ThreadMain() {
  MigrateDB();
  should_exit_ = true;
  LOG(INFO) << GetDBTypeString(type_) << " keys have been dispatched completly";
  return nullptr;
}
