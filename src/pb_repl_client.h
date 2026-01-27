#ifndef PB_REPL_CLIENT_H_
#define PB_REPL_CLIENT_H_

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "checkpoint.h"

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
  bool SendBinlogSyncAck(const Offset& offset, int32_t session_id, bool first_send);
  bool StartBinlogSyncLoop(const Offset& start_offset, int32_t session_id);
  bool PerformFullSync(Offset* new_offset);
  bool LoadBgsaveInfo(Offset* offset);
  Offset GetStartOffset() const;
  void UpdateLoggerOffset(const Offset& offset);

  static bool OffsetNewer(const Offset& a, const Offset& b);

 private:
  PikaPort* pika_port_;
  std::atomic<bool> should_stop_{false};
  std::thread thread_;
  std::unique_ptr<net::NetCli> repl_cli_;
  std::string local_ip_;
};

#endif  // PB_REPL_CLIENT_H_
