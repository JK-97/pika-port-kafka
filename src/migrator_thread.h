#ifndef MIGRATOR_THREAD_H_
#define MIGRATOR_THREAD_H_

#include <iostream>

#include "conf.h"
#include "kafka_sender.h"

namespace storage {
class Storage;
}

class MigratorThread : public net::Thread {
 public:
  MigratorThread(storage::Storage* db, std::vector<KafkaSender*>* senders, int type, int thread_num)
      : db_(db),
        should_exit_(false),
        senders_(senders),
        type_(type),
        thread_num_(thread_num),
        thread_index_(0),
        num_(0) {}

  virtual ~MigratorThread();

  int64_t num() {
    std::lock_guard l(num_mutex_);
    return num_;
  }

  int64_t keys_scanned() {
    std::lock_guard l(num_mutex_);
    return keys_scanned_;
  }

  void Stop() { should_exit_ = true; }
  int type() const { return type_; }

 private:
  void PlusNum() {
    std::lock_guard l(num_mutex_);
    ++num_;
  }

  void PlusKeysScanned() {
    std::lock_guard l(num_mutex_);
    ++keys_scanned_;
  }

  void DispatchRecord(const KafkaRecord& record);
  bool SendListTailSnapshot(const std::string& key, const std::string& data_type);
  bool IsPayloadTooLarge(const KafkaRecord& record) const;

  void MigrateDB();
  void MigrateStringsDB();
  void MigrateListsDB();
  void MigrateHashesDB();
  void MigrateSetsDB();
  void MigrateZsetsDB();

  virtual void* ThreadMain();

 private:
  storage::Storage* db_;
  bool should_exit_;

  std::vector<KafkaSender*>* senders_;
  int type_;
  int thread_num_;
 int thread_index_;

 int64_t num_;
 int64_t keys_scanned_{0};
 pstd::Mutex num_mutex_;
};

#endif
