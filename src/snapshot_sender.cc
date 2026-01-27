#include "snapshot_sender.h"

#include <chrono>
#include <sstream>
#include <vector>

#include <glog/logging.h>

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
  size_t thread_num = conf_.forward_thread_num;

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

  int64_t replies = 0;
  int64_t records = 0;
  for (auto* migrator : migrators) {
    records += migrator->num();
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
  LOG(INFO) << "Total records : " << records << " have been Scaned";
  LOG(INFO) << "Total events : " << replies << " delivered to kafka";
  return 0;
}
