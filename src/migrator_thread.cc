#include "migrator_thread.h"
#include "const.h"
#include "event_builder.h"

#include <unistd.h>

#include <glog/logging.h>
#include <functional>
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
      int64_t ttl = db->TTL(k);
      if (ttl > 0) {
        argv.push_back("EX");
        argv.push_back(std::to_string(ttl));
      }

      net::SerializeRedisCommand(argv, &cmd);
      KafkaRecord record = MakeSnapshotRecord(argv, DataTypeName(type_), k, cmd);
      PlusNum();
      DispatchRecord(record);
    }
  } while (cursor != 0 && !should_exit_);
}

void MigratorThread::MigrateListsDB() {
  auto* db = db_;
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

      int64_t pos = 0;
      std::vector<std::string> list;
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

        net::SerializeRedisCommand(argv, &cmd);
        KafkaRecord record = MakeSnapshotRecord(argv, DataTypeName(type_), k, cmd);
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

      int64_t ttl = db->TTL(k);
      if (ttl > 0) {
        net::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("EXPIRE");
        argv.push_back(k);
        argv.push_back(std::to_string(ttl));
        net::SerializeRedisCommand(argv, &cmd);
        KafkaRecord record = MakeSnapshotRecord(argv, DataTypeName(type_), k, cmd);
        PlusNum();
        DispatchRecord(record);
      }
    }
  } while (cursor != 0 && !should_exit_);
}

void MigratorThread::MigrateHashesDB() {
  auto* db = db_;
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

      std::vector<storage::FieldValue> fvs;
      storage::Status s = db->HGetall(k, &fvs);
      if (!s.ok()) {
        LOG(WARNING) << "db->HGetall(key:" << k << ") = " << s.ToString();
        continue;
      }

      auto it = fvs.begin();
      while (!should_exit_ && it != fvs.end()) {
        net::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("HMSET");
        argv.push_back(k);
        for (size_t idx = 0; idx < g_conf.sync_batch_num && !should_exit_ && it != fvs.end(); idx++, it++) {
          argv.push_back(it->field);
          argv.push_back(it->value);
        }

        net::SerializeRedisCommand(argv, &cmd);
        KafkaRecord record = MakeSnapshotRecord(argv, DataTypeName(type_), k, cmd);
        PlusNum();
        DispatchRecord(record);
      }

      int64_t ttl = db->TTL(k);
      if (ttl > 0) {
        net::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("EXPIRE");
        argv.push_back(k);
        argv.push_back(std::to_string(ttl));
        net::SerializeRedisCommand(argv, &cmd);
        KafkaRecord record = MakeSnapshotRecord(argv, DataTypeName(type_), k, cmd);
        PlusNum();
        DispatchRecord(record);
      }
    }
  } while (cursor != 0 && !should_exit_);
}

void MigratorThread::MigrateSetsDB() {
  auto* db = db_;
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

      std::vector<std::string> members;
      storage::Status s = db->SMembers(k, &members);
      if (!s.ok()) {
        LOG(WARNING) << "db->SMembers(key:" << k << ") = " << s.ToString();
        continue;
      }
      auto it = members.begin();
      while (!should_exit_ && it != members.end()) {
        std::string cmd;
        net::RedisCmdArgsType argv;

        argv.push_back("SADD");
        argv.push_back(k);
        for (size_t idx = 0; idx < g_conf.sync_batch_num && !should_exit_ && it != members.end(); idx++, it++) {
          argv.push_back(*it);
        }

        net::SerializeRedisCommand(argv, &cmd);
        KafkaRecord record = MakeSnapshotRecord(argv, DataTypeName(type_), k, cmd);
        PlusNum();
        DispatchRecord(record);
      }

      int64_t ttl = db->TTL(k);
      if (ttl > 0) {
        net::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("EXPIRE");
        argv.push_back(k);
        argv.push_back(std::to_string(ttl));
        net::SerializeRedisCommand(argv, &cmd);
        KafkaRecord record = MakeSnapshotRecord(argv, DataTypeName(type_), k, cmd);
        PlusNum();
        DispatchRecord(record);
      }
    }
  } while (cursor != 0 && !should_exit_);
}

void MigratorThread::MigrateZsetsDB() {
  auto* db = db_;
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

      int64_t pos = 0;
      std::vector<storage::ScoreMember> score_members;
      storage::Status s = db->ZRange(k, static_cast<int32_t>(pos),
                                     static_cast<int32_t>(pos + g_conf.sync_batch_num - 1), &score_members);
      if (!s.ok()) {
        LOG(WARNING) << "db->ZRange(key:" << k << ", pos:" << pos << ", batch size:" << g_conf.sync_batch_num
                     << ") = " << s.ToString();
        continue;
      }

      while (s.ok() && !should_exit_ && !score_members.empty()) {
        net::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("ZADD");
        argv.push_back(k);

        for (const auto& sm : score_members) {
          argv.push_back(std::to_string(sm.score));
          argv.push_back(sm.member);
        }

        net::SerializeRedisCommand(argv, &cmd);
        KafkaRecord record = MakeSnapshotRecord(argv, DataTypeName(type_), k, cmd);
        PlusNum();
        DispatchRecord(record);

        pos += g_conf.sync_batch_num;
        score_members.clear();
        s = db->ZRange(k, static_cast<int32_t>(pos),
                       static_cast<int32_t>(pos + g_conf.sync_batch_num - 1), &score_members);
        if (!s.ok()) {
          LOG(WARNING) << "db->ZRange(key:" << k << ", pos:" << pos << ", batch size:" << g_conf.sync_batch_num
                       << ") = " << s.ToString();
        }
      }

      int64_t ttl = db->TTL(k);
      if (ttl > 0) {
        net::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("EXPIRE");
        argv.push_back(k);
        argv.push_back(std::to_string(ttl));
        net::SerializeRedisCommand(argv, &cmd);
        KafkaRecord record = MakeSnapshotRecord(argv, DataTypeName(type_), k, cmd);
        PlusNum();
        DispatchRecord(record);
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
