#ifndef PB_REPL_CLIENT_H_
#define PB_REPL_CLIENT_H_

#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "checkpoint.h"
#include "kafka_sender.h"

namespace net {
class NetCli;
}

class PikaPort;

class PbReplClient {
 public:
  explicit PbReplClient(PikaPort* pika_port);
  ~PbReplClient();

  void Start();
  void Stop();

 private:
  struct Offset {
    uint32_t filenum{0};
    uint64_t offset{0};
  };

  void ThreadMain();
  bool ResolveLocalIp(std::string* local_ip);
  bool ConnectRepl();
  bool SendMetaSync();
  bool SendTrySync(const Offset& offset, int32_t* session_id, int* reply_code);
  bool SendDBSync(const Offset& offset, int32_t* session_id);
  bool SendBinlogSyncAck(const Offset& range_start, const Offset& range_end, int32_t session_id, bool first_send);
  bool StartBinlogSyncLoop(const Offset& start_offset, int32_t session_id);
  bool PerformFullSync(Offset* new_offset);
  bool LoadBgsaveInfo(Offset* offset);
  Offset GetStartOffset() const;
  void UpdateLoggerOffset(const Offset& offset);
  void UpdateProcessedOffset(const Offset& offset);
  bool GetProcessedOffset(Offset* out);
  void StartAckKeepalive(int32_t session_id, const Offset& start_offset);
  void StopAckKeepalive();
  void AckKeepaliveLoop();

  static bool OffsetNewer(const Offset& a, const Offset& b);

  struct BinlogTask {
    uint64_t seq{0};
    Offset ack_offset;
    std::string binlog;
  };

  struct BinlogResult {
    uint64_t seq{0};
    Offset ack_offset;
    bool ackable{true};
    bool send_to_kafka{false};
    bool advance_checkpoint{false};
    KafkaRecord record;
    Checkpoint checkpoint;
  };

  class BlockingQueue {
   public:
    explicit BlockingQueue(size_t capacity) : capacity_(capacity) {}
    bool Push(BinlogTask&& task);
    bool Push(BinlogResult&& result);
    bool Pop(BinlogTask* out);
    bool Pop(BinlogResult* out);
    bool TryPop(BinlogResult* out);
    void Stop();
    void Reset(size_t capacity);

   private:
    template <typename T>
    bool PushImpl(T&& item, std::deque<T>* queue);
    template <typename T>
    bool PopImpl(T* out, std::deque<T>* queue);

    size_t capacity_{0};
    bool stopped_{false};
    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<BinlogTask> task_queue_;
    std::deque<BinlogResult> result_queue_;
  };

  void StartWorkers();
  void StopWorkers();
  void WorkerLoop();
  void DrainResults(std::map<uint64_t, BinlogResult>* pending_results, uint64_t* next_seq,
                    const Offset& last_sent_ack, bool* has_pending_ack, Offset* pending_ack_start);

 private:
  PikaPort* pika_port_;
  std::atomic<bool> should_stop_{false};
  std::thread thread_;
  std::unique_ptr<net::NetCli> repl_cli_;
  std::string local_ip_;
  std::mutex repl_send_mu_;
  std::mutex processed_mu_;
  Offset last_processed_;
  bool has_processed_{false};

  std::atomic<bool> ack_stop_{false};
  std::thread ack_thread_;
  std::mutex ack_mu_;
  std::condition_variable ack_cv_;
  struct AckState {
    bool active{false};
    int32_t session_id{0};
    Offset last_sent;
  };
  AckState ack_state_;

  std::unique_ptr<BlockingQueue> worker_queue_;
  std::vector<std::thread> worker_threads_;
  std::atomic<bool> workers_stop_{false};
};

#endif  // PB_REPL_CLIENT_H_
