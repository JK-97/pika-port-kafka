// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <arpa/inet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <cassert>
#include <chrono>
#include <memory>
#include <string_view>

#include <glog/logging.h>
#include <functional>

#include "conf.h"
#include "const.h"
#include "event_filter.h"
#include "event_builder.h"
#include "net/include/net_cli.h"
#include "pika_port.h"
#include "pstd/include/env.h"
#include "pstd/include/rsync.h"
#include "pstd/include/pstd_string.h"

namespace {

bool ShouldSendEvent(std::string_view key, std::string_view type, std::string_view action) {
  const EventFilter* filter = g_conf.event_filter.get();
  if (!filter) {
    return true;
  }
  return filter->ShouldSend(key, type, action);
}

void AdvanceCheckpoint(CheckpointManager* checkpoint_manager, const Checkpoint& cp) {
  if (!checkpoint_manager) {
    return;
  }
  checkpoint_manager->OnFiltered(cp);
}

}  // namespace

PikaPort::PikaPort(std::string& master_ip, int master_port, std::string& passwd)
    : ping_thread_(nullptr),
      master_ip_(master_ip),
      master_port_(master_port),
      master_connection_(0),
      role_(PIKA_ROLE_PORT),
      repl_state_(PIKA_REPL_NO_CONNECT),
      requirepass_(passwd),
      should_exit_(false),
      sid_(0),
      checkpoint_manager_(nullptr),
      pb_repl_client_(nullptr),
      use_pb_sync_(false),
      heartbeat_stop_(false) {
  // Init ip host
  if (!Init()) {
    LOG(FATAL) << "Init iotcl error";
  }

  checkpoint_manager_ = new CheckpointManager(g_conf.checkpoint_path, g_conf.source_id,
                                              g_conf.kafka_topic_offsets, g_conf.kafka_brokers);

  size_t thread_num = g_conf.kafka_sender_threads;
  for (size_t i = 0; i < thread_num; i++) {
    senders_.emplace_back(new KafkaSender(static_cast<int>(i), g_conf, checkpoint_manager_));
  }

  // Create thread
  binlog_receiver_thread_ = new BinlogReceiverThread(g_conf.local_ip, g_conf.local_port + 1000, 1000);
  trysync_thread_ = new TrysyncThread();

  logger_ = new Binlog(g_conf.log_path, 104857600);
  checkpoint_manager_->SetBinlog(logger_);
}

PikaPort::~PikaPort() {
  LOG(INFO) << "Ending...";
  delete trysync_thread_;
  delete ping_thread_;
  sleep(1);
  delete binlog_receiver_thread_;
  delete pb_repl_client_;

  delete logger_;
  delete checkpoint_manager_;

  LOG(INFO) << "PikaPort " << pthread_self() << " exit!!!";
}

bool PikaPort::Init() {
  // LOG(INFO) << "host: " << g_conf.local_ip << " port: " << g_conf.local_port;
  return true;
}

void PikaPort::Cleanup() {
  StopHeartbeat();
  // shutdown server
  if (ping_thread_) {
    ping_thread_->StopThread();
  }
  if (use_pb_sync_) {
    if (pb_repl_client_) {
      pb_repl_client_->Stop();
    }
  } else {
    trysync_thread_->Stop();
  }
  size_t thread_num = g_conf.kafka_sender_threads;
  for (size_t i = 0; i < thread_num; i++) {
    senders_[i]->Stop();
  }
  for (size_t i = 0; i < thread_num; i++) {
    // senders_[i]->set_should_stop();
    senders_[i]->JoinThread();
  }
  int64_t replies = 0;
  for (size_t i = 0; i < thread_num; i++) {
    replies += senders_[i]->elements();
    delete senders_[i];
  }
  LOG(INFO) << "=============== Syncing =====================";
  LOG(INFO) << "Total events : " << replies << " delivered to kafka";

  if (checkpoint_manager_) {
    checkpoint_manager_->FlushFiltered();
  }
  delete this;  // PikaPort is a global object
  // ::google::ShutdownGoogleLogging();
}

void PikaPort::StartHeartbeat() {
  if (g_conf.heartbeat_interval_ms <= 0) {
    return;
  }
  heartbeat_stop_.store(false);
  heartbeat_thread_ = std::thread(&PikaPort::HeartbeatLoop, this);
}

void PikaPort::StopHeartbeat() {
  heartbeat_stop_.store(true);
  heartbeat_cv_.notify_all();
  if (heartbeat_thread_.joinable()) {
    heartbeat_thread_.join();
  }
}

void PikaPort::LogHeartbeat() {
  std::string protocol = use_pb_sync_ ? "pb" : "legacy";
  Checkpoint cp;
  if (checkpoint_manager_ && checkpoint_manager_->GetLast(&cp)) {
    LOG(INFO) << "Heartbeat: protocol=" << protocol
              << " stream=" << g_conf.kafka_stream_mode
              << " checkpoint=" << cp.filenum << ":" << cp.offset
              << " logic_id=" << cp.logic_id
              << " ts_ms=" << cp.ts_ms;
  } else {
    LOG(INFO) << "Heartbeat: protocol=" << protocol
              << " stream=" << g_conf.kafka_stream_mode
              << " checkpoint=none";
  }
}

void PikaPort::LogKafkaStats() {
  if (senders_.empty()) {
    return;
  }
  KafkaStatsMode mode = g_conf.kafka_stats_mode;
  bool show_agg = mode == KafkaStatsMode::kAggregated || mode == KafkaStatsMode::kAll;
  bool show_detail = mode == KafkaStatsMode::kPerSender || mode == KafkaStatsMode::kAll;
  if (!show_agg && !show_detail) {
    return;
  }
  if (last_sender_stats_.size() != senders_.size()) {
    last_sender_stats_.assign(senders_.size(), KafkaStatsTotals{});
    has_kafka_stats_ = false;
  }

  auto now = std::chrono::steady_clock::now();
  KafkaStatsTotals current;
  auto safe_delta = [](uint64_t current_value, uint64_t last_value) -> uint64_t {
    return current_value >= last_value ? (current_value - last_value) : 0;
  };
  double elapsed_sec = 0.0;
  if (has_kafka_stats_) {
    elapsed_sec = std::chrono::duration_cast<std::chrono::duration<double>>(now - last_kafka_stats_time_).count();
  }

  for (size_t i = 0; i < senders_.size(); ++i) {
    auto* sender = senders_[i];
    if (!sender) {
      continue;
    }
    auto snapshot = sender->GetStatsSnapshot();
    KafkaStatsTotals sender_current;
    sender_current.queue = snapshot.queue_size;
    sender_current.outq = snapshot.outq_len;
    sender_current.send_total = snapshot.send_total;
    sender_current.ack_total = snapshot.ack_total;
    sender_current.ack_err_total = snapshot.ack_err_total;
    sender_current.produce_err_total = snapshot.produce_err_total;

    current.queue += sender_current.queue;
    current.outq += sender_current.outq;
    current.send_total += sender_current.send_total;
    current.ack_total += sender_current.ack_total;
    current.ack_err_total += sender_current.ack_err_total;
    current.produce_err_total += sender_current.produce_err_total;

    if (show_detail && has_kafka_stats_ && elapsed_sec > 0 && i < last_sender_stats_.size()) {
      const auto& last_sender = last_sender_stats_[i];
      uint64_t send_delta = safe_delta(sender_current.send_total, last_sender.send_total);
      uint64_t ack_delta = safe_delta(sender_current.ack_total, last_sender.ack_total);
      uint64_t ack_err_delta = safe_delta(sender_current.ack_err_total, last_sender.ack_err_total);
      uint64_t produce_err_delta = safe_delta(sender_current.produce_err_total, last_sender.produce_err_total);
      double send_rate = static_cast<double>(send_delta) / elapsed_sec;
      double ack_rate = static_cast<double>(ack_delta) / elapsed_sec;
      LOG(INFO) << "KafkaSenderStats: id=" << i
                << " queue=" << sender_current.queue
                << " outq=" << sender_current.outq
                << " send_rate=" << send_rate
                << " ack_rate=" << ack_rate
                << " send_total=" << sender_current.send_total
                << " ack_total=" << sender_current.ack_total
                << " ack_err=" << ack_err_delta
                << " produce_err=" << produce_err_delta;
    }

    if (i < last_sender_stats_.size()) {
      last_sender_stats_[i] = sender_current;
    }
  }

  if (!has_kafka_stats_) {
    last_kafka_stats_ = current;
    last_kafka_stats_time_ = now;
    has_kafka_stats_ = true;
    return;
  }

  if (elapsed_sec <= 0) {
    last_kafka_stats_ = current;
    last_kafka_stats_time_ = now;
    return;
  }

  if (show_agg) {
    uint64_t send_delta = safe_delta(current.send_total, last_kafka_stats_.send_total);
    uint64_t ack_delta = safe_delta(current.ack_total, last_kafka_stats_.ack_total);
    uint64_t ack_err_delta = safe_delta(current.ack_err_total, last_kafka_stats_.ack_err_total);
    uint64_t produce_err_delta = safe_delta(current.produce_err_total, last_kafka_stats_.produce_err_total);
    double send_rate = static_cast<double>(send_delta) / elapsed_sec;
    double ack_rate = static_cast<double>(ack_delta) / elapsed_sec;
    LOG(INFO) << "KafkaStats: queue=" << current.queue
              << " outq=" << current.outq
              << " send_rate=" << send_rate
              << " ack_rate=" << ack_rate
              << " send_total=" << current.send_total
              << " ack_total=" << current.ack_total
              << " ack_err=" << ack_err_delta
              << " produce_err=" << produce_err_delta;
  }

  last_kafka_stats_ = current;
  last_kafka_stats_time_ = now;
}

void PikaPort::HeartbeatLoop() {
  const auto interval = std::chrono::milliseconds(g_conf.heartbeat_interval_ms);
  while (!heartbeat_stop_.load()) {
    std::unique_lock<std::mutex> lock(heartbeat_mutex_);
    heartbeat_cv_.wait_for(lock, interval, [this]() { return heartbeat_stop_.load(); });
    if (heartbeat_stop_.load()) {
      break;
    }
    LogHeartbeat();
    LogKafkaStats();
  }
}

void PikaPort::Start() {
  // start redis sender threads
  size_t thread_num = g_conf.kafka_sender_threads;
  for (size_t i = 0; i < thread_num; i++) {
    senders_[i]->StartThread();
  }

  // if (g_conf.filenum >= 0 && g_conf.filenum != UINT32_MAX && g_conf.offset >= 0) {
  if (g_conf.filenum != UINT32_MAX) {
    logger_->SetProducerStatus(g_conf.filenum, g_conf.offset);
  }

  auto should_use_pb = [&]() -> bool {
    if (g_conf.sync_protocol == "pb") {
      return true;
    }
    if (g_conf.sync_protocol == "legacy") {
      return false;
    }
    std::unique_ptr<net::NetCli> cli(net::NewPbCli());
    cli->set_connect_timeout(500);
    if (cli->Connect(master_ip_, master_port_ + kPortShiftReplServer, "").ok()) {
      cli->Close();
      return true;
    }
    return false;
  };

  use_pb_sync_ = should_use_pb();
  if (use_pb_sync_) {
    pb_repl_client_ = new PbReplClient(this);
    pb_repl_client_->Start();
    LOG(INFO) << "Using PB replication protocol";
  } else {
    trysync_thread_->StartThread();
    binlog_receiver_thread_->StartThread();
    SetMaster(master_ip_, master_port_);
    LOG(INFO) << "Using legacy trysync protocol";
  }

  StartHeartbeat();

  mutex_.lock();
  mutex_.lock();
  mutex_.unlock();
  LOG(INFO) << "Goodbye...";
  Cleanup();
}

void PikaPort::Stop() { mutex_.unlock(); }

std::string PikaPort::SelectTopicForEvent(const std::string& event_type) const {
  if (g_conf.kafka_stream_mode == "single") {
    return g_conf.kafka_topic_single;
  }
  if (event_type == "snapshot") {
    return g_conf.kafka_topic_snapshot;
  }
  return g_conf.kafka_topic_binlog;
}

int PikaPort::PublishSnapshotEvent(const net::RedisCmdArgsType& argv,
                                   const std::string& raw_resp,
                                   const std::string& data_type,
                                   const std::string& key) {
  std::string_view resolved_key_view = key;
  if (resolved_key_view.empty() && argv.size() > 1) {
    resolved_key_view = argv[1];
  }
  std::string_view action_view;
  if (!argv.empty()) {
    action_view = argv[0];
  }
  if (!ShouldSendEvent(resolved_key_view, data_type, action_view)) {
    return 0;
  }
  std::string resolved_key(resolved_key_view);
  std::string payload = BuildSnapshotEventJson(argv, g_conf.db_name, data_type, g_conf.source_id, raw_resp,
                                               resolved_key);
  KafkaRecord record;
  record.topic = SelectTopicForEvent("snapshot");
  record.key = BuildPartitionKey(g_conf.db_name, data_type, resolved_key);
  record.payload = std::move(payload);
  record.has_checkpoint = false;

  if (senders_.empty()) {
    return -1;
  }
  size_t idx = 0;
  if (!record.key.empty()) {
    idx = std::hash<std::string>()(record.key) % senders_.size();
  }
  senders_[idx]->Enqueue(record);
  return 0;
}

int PikaPort::PublishBinlogEvent(const net::RedisCmdArgsType& argv,
                                 const PortBinlogItem& item,
                                 const std::string& raw_resp,
                                 const std::string& key) {
  std::string_view resolved_key_view = key;
  if (resolved_key_view.empty() && argv.size() > 1) {
    resolved_key_view = argv[1];
  }
  std::string_view action_view;
  if (!argv.empty()) {
    action_view = argv[0];
  }
  std::string data_type = CommandDataType(argv.empty() ? "" : argv[0]);
  if (!ShouldSendEvent(resolved_key_view, data_type, action_view)) {
    Checkpoint cp;
    cp.filenum = item.filenum();
    cp.offset = item.offset();
    cp.logic_id = item.logic_id();
    cp.server_id = item.server_id();
    cp.term_id = 0;
    cp.ts_ms = static_cast<uint64_t>(item.exec_time()) * 1000;
    AdvanceCheckpoint(checkpoint_manager_, cp);
    return 0;
  }
  std::string resolved_key(resolved_key_view);
  std::string payload = BuildBinlogEventJson(argv, item, g_conf.db_name, data_type, g_conf.source_id, raw_resp,
                                             resolved_key);

  KafkaRecord record;
  record.topic = SelectTopicForEvent("binlog");
  record.key = BuildPartitionKey(g_conf.db_name, data_type, resolved_key);
  record.payload = std::move(payload);
  record.has_checkpoint = true;
  record.checkpoint.filenum = item.filenum();
  record.checkpoint.offset = item.offset();
  record.checkpoint.logic_id = item.logic_id();
  record.checkpoint.server_id = item.server_id();
  record.checkpoint.term_id = 0;
  record.checkpoint.ts_ms = static_cast<uint64_t>(item.exec_time()) * 1000;

  if (senders_.empty()) {
    return -1;
  }
  size_t idx = 0;
  if (!record.key.empty()) {
    idx = std::hash<std::string>()(record.key) % senders_.size();
  }
  senders_[idx]->Enqueue(record);
  return 0;
}

int PikaPort::PublishBinlogEvent(const net::RedisCmdArgsType& argv,
                                 const BinlogItem& item,
                                 const std::string& raw_resp,
                                 const std::string& key) {
  std::string_view resolved_key_view = key;
  if (resolved_key_view.empty() && argv.size() > 1) {
    resolved_key_view = argv[1];
  }
  std::string_view action_view;
  if (!argv.empty()) {
    action_view = argv[0];
  }
  std::string data_type = CommandDataType(argv.empty() ? "" : argv[0]);
  if (!ShouldSendEvent(resolved_key_view, data_type, action_view)) {
    Checkpoint cp;
    cp.filenum = item.filenum();
    cp.offset = item.offset();
    cp.logic_id = item.logic_id();
    cp.server_id = 0;
    cp.term_id = item.term_id();
    cp.ts_ms = static_cast<uint64_t>(item.exec_time()) * 1000;
    AdvanceCheckpoint(checkpoint_manager_, cp);
    return 0;
  }
  std::string resolved_key(resolved_key_view);
  std::string payload = BuildBinlogEventJson(argv, item, g_conf.db_name, data_type, g_conf.source_id, raw_resp,
                                             resolved_key);

  KafkaRecord record;
  record.topic = SelectTopicForEvent("binlog");
  record.key = BuildPartitionKey(g_conf.db_name, data_type, resolved_key);
  record.payload = std::move(payload);
  record.has_checkpoint = true;
  record.checkpoint.filenum = item.filenum();
  record.checkpoint.offset = item.offset();
  record.checkpoint.logic_id = item.logic_id();
  record.checkpoint.server_id = 0;
  record.checkpoint.term_id = item.term_id();
  record.checkpoint.ts_ms = static_cast<uint64_t>(item.exec_time()) * 1000;

  if (senders_.empty()) {
    return -1;
  }
  size_t idx = 0;
  if (!record.key.empty()) {
    idx = std::hash<std::string>()(record.key) % senders_.size();
  }
  senders_[idx]->Enqueue(record);
  return 0;
}

bool PikaPort::SetMaster(std::string& master_ip, int master_port) {
  std::lock_guard l(state_protector_);
  if (((role_ ^ PIKA_ROLE_SLAVE) != 0) && repl_state_ == PIKA_REPL_NO_CONNECT) {
    master_ip_ = master_ip;
    master_port_ = master_port;
    // role_ |= PIKA_ROLE_SLAVE;
    role_ = PIKA_ROLE_PORT;
    repl_state_ = PIKA_REPL_CONNECT;
    LOG(INFO) << "set role_ = PIKA_ROLE_PORT, repl_state_ = PIKA_REPL_CONNECT";
    return true;
  }

  return false;
}

bool PikaPort::ShouldConnectMaster() {
  std::shared_lock l(state_protector_);
  // LOG(INFO) << "repl_state: " << PikaState(repl_state_)
  //            << " role: " << PikaRole(role_)
  //   		 << " master_connection: " << master_connection_;
  return repl_state_ == PIKA_REPL_CONNECT;
}

void PikaPort::ConnectMasterDone() {
  std::lock_guard l(state_protector_);
  if (repl_state_ == PIKA_REPL_CONNECT) {
    repl_state_ = PIKA_REPL_CONNECTING;
  }
}

bool PikaPort::ShouldStartPingMaster() {
  std::shared_lock l(state_protector_);
  LOG(INFO) << "ShouldStartPingMaster: master_connection " << master_connection_ << ", repl_state "
            << PikaState(repl_state_);
  return repl_state_ == PIKA_REPL_CONNECTING && master_connection_ < 2;
}

void PikaPort::MinusMasterConnection() {
  std::lock_guard l(state_protector_);
  if (master_connection_ > 0) {
    if ((--master_connection_) <= 0) {
      // two connection with master has been deleted
      if (((role_ & PIKA_ROLE_SLAVE) != 0) || ((role_ & PIKA_ROLE_PORT) != 0)) {
        // not change by slaveof no one, so set repl_state = PIKA_REPL_CONNECT, continue to connect master
        repl_state_ = PIKA_REPL_CONNECT;
      } else {
        // change by slaveof no one, so set repl_state = PIKA_REPL_NO_CONNECT, reset to SINGLE state
        repl_state_ = PIKA_REPL_NO_CONNECT;
      }
      master_connection_ = 0;
    }
  }
}

void PikaPort::PlusMasterConnection() {
  std::lock_guard l(state_protector_);
  if (master_connection_ < 2) {
    if ((++master_connection_) >= 2) {
      // two connection with master has been established
      repl_state_ = PIKA_REPL_CONNECTED;
      LOG(INFO) << "Start Sync...";
      master_connection_ = 2;
    }
  }
}

bool PikaPort::ShouldAccessConnAsMaster(const std::string& ip) {
  std::shared_lock l(state_protector_);
  LOG(INFO) << "ShouldAccessConnAsMaster, repl_state_: " << PikaState(repl_state_) << ", ip: " << ip
            << ", master_ip: " << master_ip_;
  return repl_state_ != PIKA_REPL_NO_CONNECT && ip == master_ip_;
}

void PikaPort::RemoveMaster() {
  {
    std::lock_guard l(state_protector_);
    repl_state_ = PIKA_REPL_NO_CONNECT;
    role_ &= ~PIKA_ROLE_SLAVE;
    master_ip_ = "";
    master_port_ = -1;
  }
  if (ping_thread_) {
    int err = ping_thread_->StopThread();
    if (err != 0) {
      LOG(WARNING) << "can't join thread " << strerror(err);
    }
    delete ping_thread_;
    ping_thread_ = nullptr;
  }
}

bool PikaPort::IsWaitingDBSync() {
  std::shared_lock l(state_protector_);
  return repl_state_ == PIKA_REPL_WAIT_DBSYNC;
}

void PikaPort::NeedWaitDBSync() {
  std::lock_guard l(state_protector_);
  repl_state_ = PIKA_REPL_WAIT_DBSYNC;
}

void PikaPort::WaitDBSyncFinish() {
  std::lock_guard l(state_protector_);
  if (repl_state_ == PIKA_REPL_WAIT_DBSYNC) {
    repl_state_ = PIKA_REPL_CONNECT;
  }
}
