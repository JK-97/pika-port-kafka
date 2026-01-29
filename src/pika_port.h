// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef BINLOG_SYNC_H_
#define BINLOG_SYNC_H_

#include "binlog_receiver_thread.h"
#include "binlog_transverter.h"
#include "checkpoint.h"
#include "kafka_sender.h"
#include "pika_binlog.h"
#include "pika_binlog_transverter.h"
#include "pika_define.h"
#include "pb_repl_client.h"
#include "pstd/include/pstd_mutex.h"
#include "pstd/include/pstd_status.h"
#include "slaveping_thread.h"
#include "trysync_thread.h"
#include "net/include/redis_cli.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>

using pstd::Slice;
using pstd::Status;

class PikaPort {
 public:
  PikaPort(std::string& master_ip, int master_port, std::string& passwd);
  ~PikaPort();

  /*
   * Get & Set
   */
  std::string& master_ip() { return master_ip_; }
  int master_port() { return master_port_; }

  int64_t sid() { return sid_; }

  void SetSid(int64_t sid) { sid_ = sid; }

  int role() {
    std::shared_lock l(state_protector_);
    return role_;
  }
  int repl_state() {
    std::shared_lock l(state_protector_);
    return repl_state_;
  }
  std::string requirepass() { return requirepass_; }

  BinlogReceiverThread* binlog_receiver_thread() { return binlog_receiver_thread_; }
  TrysyncThread* trysync_thread() { return trysync_thread_; }
  Binlog* logger() { return logger_; }
  CheckpointManager* checkpoint_manager() { return checkpoint_manager_; }

  int PublishSnapshotEvent(const net::RedisCmdArgsType& argv,
                           const std::string& raw_resp,
                           const std::string& data_type,
                           const std::string& key);
  int PublishBinlogEvent(const net::RedisCmdArgsType& argv,
                         const PortBinlogItem& item,
                         const std::string& raw_resp,
                         const std::string& key);
  int PublishBinlogEvent(const net::RedisCmdArgsType& argv,
                         const BinlogItem& item,
                         const std::string& raw_resp,
                         const std::string& key);
  std::string SelectTopicForEvent(const std::string& event_type) const;

  bool SetMaster(std::string& master_ip, int master_port);
  bool ShouldConnectMaster();
  void ConnectMasterDone();
  bool ShouldStartPingMaster();
  void MinusMasterConnection();
  void PlusMasterConnection();
  bool ShouldAccessConnAsMaster(const std::string& ip);
  void RemoveMaster();
  bool IsWaitingDBSync();
  void NeedWaitDBSync();
  void WaitDBSyncFinish();

  void Start();
 void Stop();
  void Cleanup();

  bool Init();
  SlavepingThread* ping_thread_;

 private:
  void StartHeartbeat();
  void StopHeartbeat();
  void HeartbeatLoop();
  void LogHeartbeat();
  void LogKafkaStats();

  std::string master_ip_;
  int master_port_;
  int master_connection_;
  int role_;
  int repl_state_;
  std::string requirepass_;
  std::string log_path_;
  std::string dump_path_;
  std::shared_mutex rwlock_;

  pstd::Mutex mutex_;  // double lock to block main thread

  std::vector<KafkaSender*> senders_;
  CheckpointManager* checkpoint_manager_;

  bool should_exit_;

  // Master use
  int64_t sid_;

  BinlogReceiverThread* binlog_receiver_thread_;
  TrysyncThread* trysync_thread_;
  PbReplClient* pb_repl_client_;
  bool use_pb_sync_;

  Binlog* logger_;

  std::shared_mutex state_protector_;  // protect below, use for master-slave mode

  std::thread heartbeat_thread_;
  std::atomic<bool> heartbeat_stop_{false};
  std::mutex heartbeat_mutex_;
  std::condition_variable heartbeat_cv_;
  struct KafkaStatsTotals {
    int64_t queue{0};
    int64_t outq{0};
    uint64_t send_total{0};
    uint64_t ack_total{0};
    uint64_t ack_err_total{0};
    uint64_t produce_err_total{0};
  };
  KafkaStatsTotals last_kafka_stats_;
  std::vector<KafkaStatsTotals> last_sender_stats_;
  std::chrono::steady_clock::time_point last_kafka_stats_time_;
  bool has_kafka_stats_{false};

  PikaPort(PikaPort& bs);
  void operator=(const PikaPort& bs);
};

#endif
