#include "pb_repl_client.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <ctime>
#include <fstream>
#include <type_traits>
#include <utility>

#include <glog/logging.h>

#include "conf.h"
#include "event_builder.h"
#include "event_filter.h"
#include "net/include/net_cli.h"
#include "net/include/redis_cli.h"
#include "pika_binlog_transverter.h"
#include "pika_define.h"
#include "pika_inner_message.pb.h"
#include "pika_port.h"
#include "pstd/include/env.h"
#include "pstd/include/pstd_string.h"
#include "resp_parser.h"
#include "rsync_client_simple.h"
#include "snapshot_sender.h"

namespace {

const size_t kRsyncChunkBytes = 4 * 1024 * 1024;
const int kRsyncTimeoutMs = 30000;
const int kReplConnectTimeoutMs = 1500;
const int kReplRecvTimeoutMs = 1000;
const int kAckIntervalMs = 1000;

std::string BuildDumpPath(const std::string& root, const std::string& db_name) {
  if (root.empty()) {
    return db_name;
  }
  if (root.back() == '/') {
    return root + db_name;
  }
  return root + "/" + db_name;
}

}  // namespace

bool PbReplClient::BlockingQueue::Push(BinlogTask&& task) {
  return PushImpl(std::move(task), &task_queue_);
}

bool PbReplClient::BlockingQueue::Push(BinlogResult&& result) {
  return PushImpl(std::move(result), &result_queue_);
}

bool PbReplClient::BlockingQueue::Pop(BinlogTask* out) {
  return PopImpl(out, &task_queue_);
}

bool PbReplClient::BlockingQueue::Pop(BinlogResult* out) {
  return PopImpl(out, &result_queue_);
}

bool PbReplClient::BlockingQueue::TryPop(BinlogResult* out) {
  if (!out) {
    return false;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  if (result_queue_.empty()) {
    return false;
  }
  *out = std::move(result_queue_.front());
  result_queue_.pop_front();
  cv_.notify_all();
  return true;
}

void PbReplClient::BlockingQueue::Stop() {
  std::lock_guard<std::mutex> lock(mutex_);
  stopped_ = true;
  cv_.notify_all();
}

void PbReplClient::BlockingQueue::Reset(size_t capacity) {
  std::lock_guard<std::mutex> lock(mutex_);
  capacity_ = capacity;
  stopped_ = false;
  task_queue_.clear();
  result_queue_.clear();
}

template <typename T>
bool PbReplClient::BlockingQueue::PushImpl(T&& item, std::deque<T>* queue) {
  std::unique_lock<std::mutex> lock(mutex_);
  constexpr bool enforce_capacity = std::is_same<typename std::decay<T>::type, BinlogTask>::value;
  cv_.wait(lock, [&]() { return stopped_ || !enforce_capacity || queue->size() < capacity_; });
  if (stopped_) {
    return false;
  }
  queue->push_back(std::move(item));
  cv_.notify_all();
  return true;
}

template <typename T>
bool PbReplClient::BlockingQueue::PopImpl(T* out, std::deque<T>* queue) {
  if (!out) {
    return false;
  }
  std::unique_lock<std::mutex> lock(mutex_);
  cv_.wait(lock, [&]() { return stopped_ || !queue->empty(); });
  if (queue->empty()) {
    return false;
  }
  *out = std::move(queue->front());
  queue->pop_front();
  cv_.notify_all();
  return true;
}

PbReplClient::PbReplClient(PikaPort* pika_port) : pika_port_(pika_port) {}

PbReplClient::~PbReplClient() { Stop(); }

void PbReplClient::Start() {
  should_stop_.store(false);
  thread_ = std::thread(&PbReplClient::ThreadMain, this);
}

void PbReplClient::Stop() {
  should_stop_.store(true);
  StopAckKeepalive();
  StopWorkers();
  if (repl_cli_) {
    repl_cli_->Close();
  }
  if (thread_.joinable()) {
    thread_.join();
  }
}

void PbReplClient::StartWorkers() {
  StopWorkers();
  size_t workers = g_conf.binlog_workers <= 0 ? 1 : static_cast<size_t>(g_conf.binlog_workers);
  size_t capacity = g_conf.binlog_queue_size == 0 ? 1024 : g_conf.binlog_queue_size;
  worker_queue_.reset(new BlockingQueue(capacity));
  workers_stop_.store(false);
  worker_threads_.clear();
  worker_threads_.reserve(workers);
  for (size_t i = 0; i < workers; ++i) {
    worker_threads_.emplace_back(&PbReplClient::WorkerLoop, this);
  }
}

void PbReplClient::StopWorkers() {
  workers_stop_.store(true);
  if (worker_queue_) {
    worker_queue_->Stop();
  }
  for (auto& thread : worker_threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  worker_threads_.clear();
  worker_queue_.reset();
}

bool PbReplClient::ResolveLocalIp(std::string* local_ip) {
  *local_ip = g_conf.local_ip;
  std::unique_ptr<net::NetCli> cli(net::NewRedisCli());
  cli->set_connect_timeout(kReplConnectTimeoutMs);
  if ((cli->Connect(g_conf.master_ip, g_conf.master_port, "")).ok()) {
    struct sockaddr_in laddr;
    socklen_t llen = sizeof(laddr);
    getsockname(cli->fd(), reinterpret_cast<struct sockaddr*>(&laddr), &llen);
    std::string tmp_local_ip(inet_ntoa(laddr.sin_addr));
    if (!tmp_local_ip.empty()) {
      *local_ip = tmp_local_ip;
    }
    cli->Close();
    return true;
  }
  return false;
}

bool PbReplClient::ConnectRepl() {
  repl_cli_.reset(net::NewPbCli());
  repl_cli_->set_connect_timeout(kReplConnectTimeoutMs);
  repl_cli_->set_send_timeout(kRsyncTimeoutMs);
  repl_cli_->set_recv_timeout(kReplRecvTimeoutMs);
  pstd::Status s = repl_cli_->Connect(g_conf.master_ip, g_conf.master_port + kPortShiftReplServer, "");
  if (!s.ok()) {
    LOG(WARNING) << "pb repl: connect failed " << s.ToString();
    return false;
  }
  return true;
}

bool PbReplClient::SendMetaSync() {
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kMetaSync);
  InnerMessage::InnerRequest::MetaSync* meta_sync = request.mutable_meta_sync();
  InnerMessage::Node* node = meta_sync->mutable_node();
  node->set_ip(local_ip_);
  node->set_port(g_conf.local_port);
  if (!g_conf.passwd.empty()) {
    meta_sync->set_auth(g_conf.passwd);
  }

  std::lock_guard<std::mutex> lock(repl_send_mu_);
  pstd::Status s = repl_cli_->Send(&request);
  if (!s.ok()) {
    LOG(WARNING) << "pb repl: send MetaSync failed " << s.ToString();
    return false;
  }

  InnerMessage::InnerResponse response;
  s = repl_cli_->Recv(&response);
  if (!s.ok()) {
    LOG(WARNING) << "pb repl: recv MetaSync failed " << s.ToString();
    return false;
  }
  if (response.code() != InnerMessage::kOk) {
    LOG(WARNING) << "pb repl: MetaSync error " << response.reply();
    return false;
  }
  return true;
}

bool PbReplClient::SendTrySync(const Offset& offset, int32_t* session_id, int* reply_code) {
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kTrySync);
  InnerMessage::InnerRequest::TrySync* try_sync = request.mutable_try_sync();
  InnerMessage::Node* node = try_sync->mutable_node();
  node->set_ip(local_ip_);
  node->set_port(g_conf.local_port);
  InnerMessage::Slot* slot = try_sync->mutable_slot();
  slot->set_db_name(g_conf.db_name);
  slot->set_slot_id(0);
  InnerMessage::BinlogOffset* boffset = try_sync->mutable_binlog_offset();
  boffset->set_filenum(offset.filenum);
  boffset->set_offset(offset.offset);

  std::lock_guard<std::mutex> lock(repl_send_mu_);
  pstd::Status s = repl_cli_->Send(&request);
  if (!s.ok()) {
    LOG(WARNING) << "pb repl: send TrySync failed " << s.ToString();
    return false;
  }

  InnerMessage::InnerResponse response;
  int unexpected = 0;
  const int kMaxUnexpected = 10;
  while (true) {
    s = repl_cli_->Recv(&response);
    if (!s.ok()) {
      LOG(WARNING) << "pb repl: recv TrySync failed " << s.ToString();
      return false;
    }
    if (response.type() == InnerMessage::kTrySync && response.has_try_sync()) {
      break;
    }
    if (++unexpected >= kMaxUnexpected) {
      LOG(WARNING) << "pb repl: unexpected TrySync response type, exceeded retries";
      return false;
    }
    LOG(WARNING) << "pb repl: unexpected TrySync response type, waiting for TrySync";
  }
  const auto& try_sync_resp = response.try_sync();
  *reply_code = try_sync_resp.reply_code();
  *session_id = try_sync_resp.has_session_id() ? try_sync_resp.session_id() : 0;
  return true;
}

bool PbReplClient::SendDBSync(const Offset& offset, int32_t* session_id) {
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kDBSync);
  InnerMessage::InnerRequest::DBSync* db_sync = request.mutable_db_sync();
  InnerMessage::Node* node = db_sync->mutable_node();
  node->set_ip(local_ip_);
  node->set_port(g_conf.local_port);
  InnerMessage::Slot* slot = db_sync->mutable_slot();
  slot->set_db_name(g_conf.db_name);
  slot->set_slot_id(0);
  InnerMessage::BinlogOffset* boffset = db_sync->mutable_binlog_offset();
  boffset->set_filenum(offset.filenum);
  boffset->set_offset(offset.offset);

  std::lock_guard<std::mutex> lock(repl_send_mu_);
  pstd::Status s = repl_cli_->Send(&request);
  if (!s.ok()) {
    LOG(WARNING) << "pb repl: send DBSync failed " << s.ToString();
    return false;
  }

  InnerMessage::InnerResponse response;
  s = repl_cli_->Recv(&response);
  if (!s.ok()) {
    LOG(WARNING) << "pb repl: recv DBSync failed " << s.ToString();
    return false;
  }
  if (response.type() != InnerMessage::kDBSync || !response.has_db_sync()) {
    LOG(WARNING) << "pb repl: DBSync response missing";
    return false;
  }
  if (response.code() != InnerMessage::kOk) {
    LOG(WARNING) << "pb repl: DBSync error " << response.reply();
    return false;
  }
  *session_id = response.db_sync().session_id();
  return true;
}

bool PbReplClient::SendBinlogSyncAck(const Offset& range_start, const Offset& range_end, int32_t session_id,
                                     bool first_send) {
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kBinlogSync);
  InnerMessage::InnerRequest::BinlogSync* binlog_sync = request.mutable_binlog_sync();
  InnerMessage::Node* node = binlog_sync->mutable_node();
  node->set_ip(local_ip_);
  node->set_port(g_conf.local_port);
  binlog_sync->set_db_name(g_conf.db_name);
  binlog_sync->set_slot_id(0);
  binlog_sync->set_first_send(first_send);
  binlog_sync->set_session_id(session_id);

  InnerMessage::BinlogOffset* ack_start = binlog_sync->mutable_ack_range_start();
  ack_start->set_filenum(range_start.filenum);
  ack_start->set_offset(range_start.offset);
  ack_start->set_term(0);
  ack_start->set_index(0);
  InnerMessage::BinlogOffset* ack_end = binlog_sync->mutable_ack_range_end();
  ack_end->set_filenum(range_end.filenum);
  ack_end->set_offset(range_end.offset);
  ack_end->set_term(0);
  ack_end->set_index(0);

  std::lock_guard<std::mutex> lock(repl_send_mu_);
  pstd::Status s = repl_cli_->Send(&request);
  if (!s.ok()) {
    LOG(WARNING) << "pb repl: send BinlogSync ack failed " << s.ToString();
    return false;
  }
  return true;
}

PbReplClient::Offset PbReplClient::GetStartOffset() const {
  Offset offset{static_cast<uint32_t>(g_conf.filenum), static_cast<uint64_t>(g_conf.offset)};
  Checkpoint cp;
  if (pika_port_->checkpoint_manager()->GetLast(&cp)) {
    offset.filenum = cp.filenum;
    offset.offset = cp.offset;
  }
  return offset;
}

void PbReplClient::UpdateLoggerOffset(const Offset& offset) {
  if (pika_port_->logger()) {
    pika_port_->logger()->SetProducerStatus(offset.filenum, offset.offset);
  }
}

bool PbReplClient::OffsetNewer(const Offset& a, const Offset& b) {
  if (a.filenum > b.filenum) {
    return true;
  }
  if (a.filenum == b.filenum && a.offset > b.offset) {
    return true;
  }
  return false;
}

void PbReplClient::UpdateProcessedOffset(const Offset& offset) {
  std::lock_guard<std::mutex> lock(processed_mu_);
  if (!has_processed_ || OffsetNewer(offset, last_processed_)) {
    last_processed_ = offset;
    has_processed_ = true;
  }
}

bool PbReplClient::GetProcessedOffset(Offset* out) {
  std::lock_guard<std::mutex> lock(processed_mu_);
  if (!has_processed_) {
    return false;
  }
  *out = last_processed_;
  return true;
}

void PbReplClient::WorkerLoop() {
  while (!workers_stop_.load()) {
    BinlogTask task;
    if (!worker_queue_ || !worker_queue_->Pop(&task)) {
      if (workers_stop_.load()) {
        break;
      }
      continue;
    }

    BinlogResult result;
    result.seq = task.seq;
    result.ack_offset = task.ack_offset;
    result.ackable = true;

    BinlogItem binlog_item;
    if (!PikaBinlogTransverter::BinlogDecode(TypeFirst, task.binlog, &binlog_item)) {
      LOG(WARNING) << "pb repl: binlog decode failed";
      if (worker_queue_) {
        worker_queue_->Push(std::move(result));
      }
      continue;
    }

    net::RedisCmdArgsType argv;
    if (ParseRedisRESPArray(binlog_item.content(), &argv) != kRespOk) {
      LOG(WARNING) << "pb repl: parse redis resp failed";
      if (worker_queue_) {
        worker_queue_->Push(std::move(result));
      }
      continue;
    }

    if (argv.empty()) {
      if (worker_queue_) {
        worker_queue_->Push(std::move(result));
      }
      continue;
    }

    std::string key;
    if (argv.size() > 1) {
      key = argv[1];
    }
    std::string command = binlog_item.content();
    if (argv[0] == "pksetexat" && argv.size() > 2) {
      std::string temp = argv[2];
      unsigned long int sec = time(nullptr);
      unsigned long int tot = std::stol(temp) - sec;
      std::string time_out = std::to_string(tot);
      command.erase(0, 4);
      command.replace(0, 13, "*4\r\n$5\r\nsetex");
      int start = 13 + 3 + static_cast<int>(std::to_string(key.size()).size()) + 2 + static_cast<int>(key.size()) + 3;
      int old_time_size = static_cast<int>(std::to_string(temp.size()).size() + 2 + temp.size());
      int new_time_size = static_cast<int>(std::to_string(time_out.size()).size() + 2 + time_out.size());
      int diff = old_time_size - new_time_size;
      command.erase(start, diff);
      std::string time_cmd = std::to_string(time_out.size()) + "\r\n" + time_out;
      command.replace(start, new_time_size, time_cmd);
    }

    std::string data_type = CommandDataType(argv[0]);
    bool should_send = true;
    const EventFilter* filter = g_conf.event_filter.get();
    if (filter) {
      should_send = filter->ShouldSend(key, data_type, argv[0]);
    }

    Checkpoint checkpoint;
    checkpoint.filenum = binlog_item.filenum();
    checkpoint.offset = binlog_item.offset();
    checkpoint.logic_id = binlog_item.logic_id();
    checkpoint.server_id = 0;
    checkpoint.term_id = binlog_item.term_id();
    checkpoint.ts_ms = static_cast<uint64_t>(binlog_item.exec_time()) * 1000;

    if (!should_send) {
      result.advance_checkpoint = true;
      result.checkpoint = checkpoint;
      if (worker_queue_) {
        worker_queue_->Push(std::move(result));
      }
      continue;
    }

    std::string payload = BuildBinlogEventJson(argv, binlog_item, g_conf.db_name, data_type, g_conf.source_id,
                                               command, key);
    KafkaRecord record;
    record.topic = pika_port_->SelectTopicForEvent("binlog");
    record.key = BuildPartitionKey(g_conf.db_name, data_type, key);
    record.payload = std::move(payload);
    record.has_checkpoint = true;
    record.checkpoint = checkpoint;

    result.send_to_kafka = true;
    result.record = std::move(record);
    if (worker_queue_) {
      worker_queue_->Push(std::move(result));
    }
  }
}

void PbReplClient::DrainResults(std::map<uint64_t, BinlogResult>* pending_results,
                                uint64_t* next_seq,
                                const Offset& last_sent_ack,
                                bool* has_pending_ack,
                                Offset* pending_ack_start) {
  if (!worker_queue_ || !pending_results || !next_seq) {
    return;
  }
  BinlogResult result;
  while (worker_queue_->TryPop(&result)) {
    (*pending_results)[result.seq] = std::move(result);
  }

  while (true) {
    auto it = pending_results->find(*next_seq);
    if (it == pending_results->end()) {
      break;
    }
    BinlogResult ready = std::move(it->second);
    pending_results->erase(it);
    if (ready.send_to_kafka) {
      int ret = pika_port_->EnqueueKafkaRecord(std::move(ready.record));
      if (ret != 0) {
        LOG(WARNING) << "pb repl: enqueue kafka record failed";
        ready.ackable = false;
      }
    } else if (ready.advance_checkpoint) {
      if (auto* checkpoint_manager = pika_port_->checkpoint_manager(); checkpoint_manager) {
        checkpoint_manager->OnFiltered(ready.checkpoint);
      }
    }
    if (ready.ackable) {
      UpdateProcessedOffset(ready.ack_offset);
      if (has_pending_ack && pending_ack_start && !*has_pending_ack &&
          OffsetNewer(ready.ack_offset, last_sent_ack)) {
        *pending_ack_start = ready.ack_offset;
        *has_pending_ack = true;
      }
    }
    (*next_seq)++;
  }
}

bool PbReplClient::LoadBgsaveInfo(Offset* offset) {
  std::string info_path = BuildDumpPath(g_conf.dump_path, g_conf.db_name);
  if (info_path.back() != '/') {
    info_path.append("/");
  }
  info_path.append("info");
  if (!pstd::FileExists(info_path)) {
    std::string alt = g_conf.dump_path;
    if (!alt.empty() && alt.back() != '/') {
      alt.append("/");
    }
    alt.append("info");
    if (!pstd::FileExists(alt)) {
      LOG(WARNING) << "pb repl: info file missing after dbsync";
      return false;
    }
    info_path = alt;
  }

  std::ifstream is(info_path);
  if (!is) {
    LOG(WARNING) << "pb repl: failed to open info file " << info_path;
    return false;
  }
  std::string line;
  int lineno = 0;
  int64_t filenum = 0;
  int64_t boffset = 0;
  int64_t tmp = 0;
  while (std::getline(is, line)) {
    lineno++;
    if (lineno == 4) {
      if (pstd::string2int(line.data(), line.size(), &tmp) == 0 || tmp < 0) {
        return false;
      }
      filenum = tmp;
    } else if (lineno == 5) {
      if (pstd::string2int(line.data(), line.size(), &tmp) == 0 || tmp < 0) {
        return false;
      }
      boffset = tmp;
    }
  }
  is.close();
  pstd::DeleteFile(info_path);

  offset->filenum = static_cast<uint32_t>(filenum);
  offset->offset = static_cast<uint64_t>(boffset);
  return true;
}

bool PbReplClient::PerformFullSync(Offset* new_offset) {
  struct FullSyncGuard {
    explicit FullSyncGuard(PikaPort* port_in) : port(port_in) {
      if (port) {
        port->SetFullSyncing(true);
      }
    }
    ~FullSyncGuard() {
      if (port) {
        port->SetFullSyncing(false);
      }
    }
    PikaPort* port;
  };
  FullSyncGuard guard(pika_port_);
  std::string dump_path = BuildDumpPath(g_conf.dump_path, g_conf.db_name);
  RsyncClientSimple rsync(g_conf.master_ip, g_conf.master_port, g_conf.db_name, dump_path,
                          kRsyncChunkBytes, kRsyncTimeoutMs);
  if (!rsync.Fetch()) {
    return false;
  }
  if (!LoadBgsaveInfo(new_offset)) {
    return false;
  }
  SnapshotSender sender(g_conf, pika_port_->checkpoint_manager());
  int sender_ret = sender.Run();
  if (sender_ret != 0) {
    LOG(WARNING) << "pb repl: snapshot sender failed";
  } else if (auto* checkpoint_manager = pika_port_->checkpoint_manager(); checkpoint_manager) {
    Checkpoint cp;
    cp.filenum = new_offset->filenum;
    cp.offset = new_offset->offset;
    checkpoint_manager->OnFiltered(cp);
    checkpoint_manager->FlushFiltered();
  }
  return true;
}

bool PbReplClient::StartBinlogSyncLoop(const Offset& start_offset, int32_t session_id) {
  Offset last_sent_ack = start_offset;
  Offset pending_ack_start;
  bool has_pending_ack = false;
  auto last_ack_time = std::chrono::steady_clock::now();
  auto last_warn_time = std::chrono::steady_clock::now();
  auto last_binlog_time = std::chrono::steady_clock::now();
  auto last_non_binlog_log = last_binlog_time;
  auto last_session_mismatch_log = last_binlog_time;
  const auto log_throttle = std::chrono::milliseconds(5000);
  if (!SendBinlogSyncAck(start_offset, start_offset, session_id, true)) {
    return false;
  }
  UpdateProcessedOffset(start_offset);
  StartAckKeepalive(session_id, start_offset);
  StartWorkers();
  uint64_t next_seq = 0;
  uint64_t seq_counter = 0;
  std::map<uint64_t, BinlogResult> pending_results;

  while (!should_stop_.load()) {
    InnerMessage::InnerResponse response;
    pstd::Status s = repl_cli_->Recv(&response);
    auto now = std::chrono::steady_clock::now();
    if (!s.ok()) {
      if (s.IsTimeout()) {
        // keep alive and ack if checkpoint advanced
      } else {
        LOG(WARNING) << "pb repl: binlog recv failed " << s.ToString();
        StopAckKeepalive();
        StopWorkers();
        return false;
      }
    } else {
      if (response.type() == InnerMessage::kBinlogSync) {
        bool matched_session = false;
        bool logged_mismatch = false;
        for (int i = 0; i < response.binlog_sync_size(); ++i) {
          const auto& binlog_res = response.binlog_sync(i);
          if (binlog_res.session_id() != session_id) {
            if (!logged_mismatch && now - last_session_mismatch_log >= log_throttle) {
              LOG(WARNING) << "pb repl: binlog session mismatch"
                           << " expected=" << session_id
                           << " got=" << binlog_res.session_id();
              last_session_mismatch_log = now;
              logged_mismatch = true;
            }
            continue;
          }
          matched_session = true;
          BinlogTask task;
          task.seq = seq_counter++;
          task.ack_offset.filenum = binlog_res.binlog_offset().filenum();
          task.ack_offset.offset = binlog_res.binlog_offset().offset();
          task.binlog = binlog_res.binlog();
          if (!worker_queue_ || !worker_queue_->Push(std::move(task))) {
            LOG(WARNING) << "pb repl: enqueue binlog task failed";
            StopAckKeepalive();
            StopWorkers();
            return false;
          }
        }
        if (matched_session) {
          last_binlog_time = now;
        }
      } else if (now - last_non_binlog_log >= log_throttle) {
        LOG(WARNING) << "pb repl: unexpected response type while waiting binlog sync"
                     << " type=" << static_cast<int>(response.type());
        last_non_binlog_log = now;
      }
    }

    DrainResults(&pending_results, &next_seq, last_sent_ack, &has_pending_ack, &pending_ack_start);

    Offset committed = last_sent_ack;
    Offset processed;
    if (GetProcessedOffset(&processed) && OffsetNewer(processed, committed)) {
      committed = processed;
    }
    auto ms_since = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_ack_time).count();
    if (g_conf.pb_ack_delay_warn_ms > 0 && ms_since >= g_conf.pb_ack_delay_warn_ms) {
      auto warn_since = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_warn_time).count();
      if (warn_since >= g_conf.pb_ack_delay_warn_ms) {
        LOG(WARNING) << "pb repl: ack delay " << ms_since << "ms"
                     << " session_id=" << session_id
                     << " last_ack=" << last_sent_ack.filenum << ":" << last_sent_ack.offset
                     << " committed=" << committed.filenum << ":" << committed.offset;
        last_warn_time = now;
      }
    }
    if (g_conf.pb_idle_timeout_ms > 0) {
      auto idle_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_binlog_time).count();
      if (idle_ms >= g_conf.pb_idle_timeout_ms) {
        LOG(WARNING) << "pb repl: idle " << idle_ms << "ms without binlog response, reconnecting"
                     << " session_id=" << session_id
                     << " last_ack=" << last_sent_ack.filenum << ":" << last_sent_ack.offset
                     << " committed=" << committed.filenum << ":" << committed.offset;
        StopAckKeepalive();
        StopWorkers();
        return false;
      }
    }
    bool send_ping = false;
    bool should_send_ack = false;
    Offset ack_start;
    Offset ack_end;
    if (has_pending_ack && OffsetNewer(committed, last_sent_ack)) {
      ack_start = pending_ack_start;
      ack_end = committed;
      should_send_ack = true;
    } else if (ms_since >= kAckIntervalMs) {
      ack_start = Offset();
      ack_end = Offset();
      send_ping = true;
      should_send_ack = true;
    }
    if (should_send_ack) {
      if (SendBinlogSyncAck(ack_start, ack_end, session_id, false)) {
        last_ack_time = now;
        if (!send_ping) {
          last_sent_ack = ack_end;
          has_pending_ack = false;
          std::lock_guard<std::mutex> lock(ack_mu_);
          if (ack_state_.active && ack_state_.session_id == session_id) {
            ack_state_.last_sent = last_sent_ack;
          }
        }
      }
    }
  }
  StopAckKeepalive();
  StopWorkers();
  return true;
}

void PbReplClient::ThreadMain() {
  ResolveLocalIp(&local_ip_);
  while (!should_stop_.load()) {
    if (!ConnectRepl()) {
      sleep(1);
      continue;
    }
    if (!SendMetaSync()) {
      repl_cli_->Close();
      sleep(1);
      continue;
    }

    Offset start_offset = GetStartOffset();
    int32_t session_id = 0;
    int reply_code = 0;
    if (!SendTrySync(start_offset, &session_id, &reply_code)) {
      repl_cli_->Close();
      sleep(1);
      continue;
    }

    if (reply_code == InnerMessage::InnerResponse::TrySync::kSyncPointBePurged) {
      int32_t dbsync_session = 0;
      if (!SendDBSync(start_offset, &dbsync_session)) {
        repl_cli_->Close();
        sleep(1);
        continue;
      }
      Offset new_offset;
      if (!PerformFullSync(&new_offset)) {
        repl_cli_->Close();
        sleep(1);
        continue;
      }
      UpdateLoggerOffset(new_offset);

      repl_cli_->Close();
      if (!ConnectRepl()) {
        sleep(1);
        continue;
      }
      if (!SendMetaSync()) {
        repl_cli_->Close();
        sleep(1);
        continue;
      }

      if (!SendTrySync(new_offset, &session_id, &reply_code)) {
        repl_cli_->Close();
        sleep(1);
        continue;
      }
      if (reply_code != InnerMessage::InnerResponse::TrySync::kOk) {
        LOG(WARNING) << "pb repl: TrySync after dbsync failed";
        repl_cli_->Close();
        sleep(1);
        continue;
      }
      start_offset = new_offset;
    } else if (reply_code != InnerMessage::InnerResponse::TrySync::kOk) {
      LOG(WARNING) << "pb repl: TrySync error code " << reply_code;
      repl_cli_->Close();
      sleep(1);
      continue;
    }

    if (!StartBinlogSyncLoop(start_offset, session_id)) {
      repl_cli_->Close();
      sleep(1);
      continue;
    }
    repl_cli_->Close();
  }
}

void PbReplClient::StartAckKeepalive(int32_t session_id, const Offset& start_offset) {
  StopAckKeepalive();
  {
    std::lock_guard<std::mutex> lock(ack_mu_);
    ack_state_.active = true;
    ack_state_.session_id = session_id;
    ack_state_.last_sent = start_offset;
  }
  ack_stop_.store(false);
  ack_thread_ = std::thread(&PbReplClient::AckKeepaliveLoop, this);
}

void PbReplClient::StopAckKeepalive() {
  ack_stop_.store(true);
  ack_cv_.notify_all();
  if (ack_thread_.joinable()) {
    ack_thread_.join();
  }
  std::lock_guard<std::mutex> lock(ack_mu_);
  ack_state_.active = false;
  ack_state_.session_id = 0;
}

void PbReplClient::AckKeepaliveLoop() {
  const auto interval = std::chrono::milliseconds(kAckIntervalMs);
  while (!ack_stop_.load()) {
    std::unique_lock<std::mutex> lock(ack_mu_);
    ack_cv_.wait_for(lock, interval, [this]() { return ack_stop_.load(); });
    if (ack_stop_.load()) {
      break;
    }
    if (!ack_state_.active) {
      continue;
    }
    int32_t session_id = ack_state_.session_id;
    lock.unlock();

    SendBinlogSyncAck(Offset(), Offset(), session_id, false);
  }
}
