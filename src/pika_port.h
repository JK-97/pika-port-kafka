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
#include "pika_define.h"
#include "pstd/include/pstd_mutex.h"
#include "pstd/include/pstd_status.h"
#include "slaveping_thread.h"
#include "trysync_thread.h"
#include "net/include/redis_cli.h"

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

  Binlog* logger_;

  std::shared_mutex state_protector_;  // protect below, use for master-slave mode

  PikaPort(PikaPort& bs);
  void operator=(const PikaPort& bs);
};

#endif
