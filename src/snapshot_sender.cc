#include "snapshot_sender.h"

#include <atomic>
#include <chrono>
#include <sstream>
#include <thread>
#include <vector>

#include <glog/logging.h>

#include "const.h"
#include "kafka_sender.h"
#include "migrator_thread.h"
#include "pstd/include/env.h"
#include "storage/storage.h"

SnapshotSender::SnapshotSender(const Conf& conf, CheckpointManager* checkpoint_manager)
    : conf_(conf), checkpoint_manager_(checkpoint_manager) {}

int SnapshotSender::Run() {
  using std::chrono::high_resolution_clock;

  std::string db_path = conf_.dump_path;
  if (db_path.empty()) {
    LOG(WARNING) << "snapshot sender: dump_path is empty";
    return -1;
  }
  if (db_path.back() != '/') {
    db_path.append("/");
  }

  high_resolution_clock::time_point start = high_resolution_clock::now();
  size_t thread_num = conf_.kafka_sender_threads;

  std::vector<KafkaSender*> senders;
  std::vector<MigratorThread*> migrators;
  senders.reserve(thread_num);

  for (size_t i = 0; i < thread_num; i++) {
    senders.emplace_back(new KafkaSender(static_cast<int>(i), conf_, checkpoint_manager_));
  }

  rocksdb::Options options;
  options.create_if_missing = true;
  options.keep_log_file_num = 10;
  options.max_manifest_file_size = 64 * 1024 * 1024;
  options.max_log_file_size = 512 * 1024 * 1024;
  options.write_buffer_size = 512 * 1024 * 1024;
  options.target_file_size_base = 40 * 1024 * 1024;

  storage::StorageOptions bw_options;
  bw_options.options = options;

  storage::Storage storage;
  std::string db_root = db_path;
  if (!conf_.db_name.empty()) {
    std::string candidate = db_root + conf_.db_name;
    if (pstd::FileExists(candidate)) {
      db_root = candidate;
    }
  }

  rocksdb::Status s = storage.Open(bw_options, db_root);
  LOG(INFO) << "Open db path " << db_root << " result " << s.ToString();
  if (s.ok()) {
    migrators.emplace_back(
        new MigratorThread(&storage, &senders, static_cast<int>(storage::DataType::kStrings), thread_num));
    migrators.emplace_back(
        new MigratorThread(&storage, &senders, static_cast<int>(storage::DataType::kLists), thread_num));
    migrators.emplace_back(
        new MigratorThread(&storage, &senders, static_cast<int>(storage::DataType::kHashes), thread_num));
    migrators.emplace_back(
        new MigratorThread(&storage, &senders, static_cast<int>(storage::DataType::kSets), thread_num));
    migrators.emplace_back(
        new MigratorThread(&storage, &senders, static_cast<int>(storage::DataType::kZSets), thread_num));
  }

  const int64_t interval_ms = conf_.heartbeat_interval_ms > 0 ? conf_.heartbeat_interval_ms : 5000;
  const auto progress_interval = std::chrono::milliseconds(interval_ms);
  auto log_progress = [&](const char* stage) {
    int64_t total_keys = 0;
    int64_t total_records = 0;
    std::ostringstream detail;
    for (size_t i = 0; i < migrators.size(); ++i) {
      auto* migrator = migrators[i];
      int64_t keys = migrator->keys_scanned();
      int64_t records = migrator->num();
      total_keys += keys;
      total_records += records;
      if (i > 0) {
        detail << " ";
      }
      detail << GetDBTypeString(migrator->type()) << ":keys=" << keys << ",records=" << records;
    }
    auto now = high_resolution_clock::now();
    auto elapsed_sec = std::chrono::duration_cast<std::chrono::seconds>(now - start).count();
    LOG(INFO) << "SnapshotProgress: " << stage
              << " elapsed=" << elapsed_sec << "s"
              << " keys_scanned=" << total_keys
              << " records_dispatched=" << total_records
              << " by_type={" << detail.str() << "}";
  };

  LOG(INFO) << "SnapshotProgress: start interval_ms=" << progress_interval.count() << " db_path=" << db_root;
  log_progress("start");

  std::atomic<bool> progress_stop{false};
  std::thread progress_thread([&]() {
    while (!progress_stop.load()) {
      std::this_thread::sleep_for(progress_interval);
      if (progress_stop.load()) {
        break;
      }
      log_progress("progress");
    }
  });

  for (auto* sender : senders) {
    sender->StartThread();
  }

  for (auto* migrator : migrators) {
    migrator->StartThread();
  }
  for (auto* migrator : migrators) {
    migrator->JoinThread();
  }

  for (auto* sender : senders) {
    sender->Stop();
  }
  for (auto* sender : senders) {
    sender->JoinThread();
  }

  progress_stop.store(true);
  if (progress_thread.joinable()) {
    progress_thread.join();
  }
  log_progress("done");

  int64_t replies = 0;
  int64_t records = 0;
  int64_t keys_scanned = 0;
  for (auto* migrator : migrators) {
    records += migrator->num();
    keys_scanned += migrator->keys_scanned();
    delete migrator;
  }
  for (auto* sender : senders) {
    replies += sender->elements();
    delete sender;
  }

  high_resolution_clock::time_point end = high_resolution_clock::now();
  std::chrono::hours hours = std::chrono::duration_cast<std::chrono::hours>(end - start);
  std::chrono::minutes minutes = std::chrono::duration_cast<std::chrono::minutes>(end - start);
  std::chrono::seconds seconds = std::chrono::duration_cast<std::chrono::seconds>(end - start);

  LOG(INFO) << "=============== Retransmitting =====================";
  LOG(INFO) << "Running time  :";
  LOG(INFO) << hours.count() << " hour " << minutes.count() - hours.count() * 60 << " min "
            << seconds.count() - hours.count() * 60 * 60 << " s";
  LOG(INFO) << "Total keys : " << keys_scanned << " scanned";
  LOG(INFO) << "Total records : " << records << " have been Scaned";
  LOG(INFO) << "Total events : " << replies << " delivered to kafka";
  return 0;
}
