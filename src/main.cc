// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

// #include <glog/logging.h>
#include <fcntl.h>
#include <getopt.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <cctype>
#include <csignal>
#include <fstream>
#include <cstdlib>
#include <random>
#include <string_view>
#include <vector>

#include <glog/logging.h>

#include "checkpoint.h"
#include "conf.h"
#include "event_filter.h"
#include "pikiwi_info.h"
#include "pika_port.h"

Conf g_conf;
PikaPort* g_pika_port;
int pidFileFd = 0;

static int lockFile(int fd) {
  struct flock lock;
  lock.l_type = F_WRLCK;
  lock.l_start = 0;
  lock.l_whence = SEEK_SET;
  lock.l_len = 0;

  return fcntl(fd, F_SETLK, &lock);
}

static void createPidFile(const char* file) {
  int flags = O_RDWR | O_CREAT;
#if !defined(OS_MACOSX) && !defined(OS_OPENBSD) && !defined(OS_SOLARIS)
    flags |= O_DIRECT;
#endif
  int fd = open(file, flags, S_IRUSR | S_IWUSR);
  if (-1 == fd) {
    LOG(FATAL) << "open(" << file << ") = " << fd;
  }

  int ret = lockFile(fd);
  if (ret < 0) {
    close(fd);
    LOG(FATAL) << "lock(" << fd << ") = " << ret;
  }

  // int pid = (int)(getpid());
  pidFileFd = fd;
}

static void daemonize() {
  if (fork() != 0) { exit(0); /* parent exits */
}
  setsid();                 /* create a new session */
}

static void close_std() {
  int fd;
  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, STDIN_FILENO);
    dup2(fd, STDOUT_FILENO);
    dup2(fd, STDERR_FILENO);
    close(fd);
  }
}

static void IntSigHandle(const int sig) {
  LOG(INFO) << "Catch Signal " << sig << ", cleanup...";
  if (2 < pidFileFd) {
    close(pidFileFd);
  }
  g_pika_port->Stop();
}

static void SignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

static void GlogInit(const std::string& log_path, bool is_daemon) {
  if (!pstd::FileExists(log_path)) {
    pstd::CreatePath(log_path);
  }

  if (!is_daemon) {
    FLAGS_alsologtostderr = true;
  }

  FLAGS_log_dir = log_path;
  FLAGS_max_log_size = 2048;  // log file 2GB
  ::google::InitGoogleLogging("pika_port_kafka");
}

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "\tPika_port_kafka reads data from pika 3.0 and send to kafka" << std::endl;
  std::cout << "\t-h     -- show this help" << std::endl;
  std::cout << "\t-C, --config=PATH  -- config file path (key=value)" << std::endl;
  std::cout << "\t-t     -- local host ip(OPTIONAL default: 127.0.0.1)" << std::endl;
  std::cout << "\t-p     -- local port(OPTIONAL)" << std::endl;
  std::cout << "\t-i     -- master ip(OPTIONAL default: 127.0.0.1)" << std::endl;
  std::cout << "\t-o     -- master port(REQUIRED)" << std::endl;
  std::cout << "\t-x     -- kafka sender threads(OPTIONAL default: 1)" << std::endl;
  std::cout << "\t-G     -- heartbeat kafka sender stats mode (none|agg|detail|all, OPTIONAL default: agg)"
            << std::endl;
  std::cout << "\t-z     -- max timeout duration for waiting pika master bgsave data (OPTIONAL default 1800s)"
            << std::endl;
  std::cout << "\t-f     -- binlog filenum(OPTIONAL default: local offset)" << std::endl;
  std::cout << "\t-s     -- binlog offset(OPTIONAL default: local offset)" << std::endl;
  std::cout << "\t-w     -- password for master(OPTIONAL)" << std::endl;
  std::cout << "\t-r     -- rsync dump data path(OPTIONAL default: ./rsync_dump)" << std::endl;
  std::cout << "\t-l     -- local log path(OPTIONAL default: ./log)" << std::endl;
  std::cout << "\t-b     -- max batch number when port rsync dump data (OPTIONAL default: 512)" << std::endl;
  std::cout << "\t-d     -- daemonize(OPTIONAL)" << std::endl;
  std::cout << "\t-e     -- exit(return -1) if dbsync start(OPTIONAL)" << std::endl;
  std::cout << "\t-k     -- kafka brokers (REQUIRED for kafka mode)" << std::endl;
  std::cout << "\t-c     -- kafka client id (OPTIONAL)" << std::endl;
  std::cout << "\t-S     -- kafka snapshot topic (OPTIONAL)" << std::endl;
  std::cout << "\t-B     -- kafka binlog topic (OPTIONAL)" << std::endl;
  std::cout << "\t-T     -- kafka single stream topic (OPTIONAL)" << std::endl;
  std::cout << "\t-O     -- kafka offsets topic (OPTIONAL)" << std::endl;
  std::cout << "\t-Q     -- enable kafka offsets topic (true|false, OPTIONAL default: true)" << std::endl;
  std::cout << "\t-P     -- checkpoint file path (OPTIONAL)" << std::endl;
  std::cout << "\t-M     -- kafka stream mode (dual|single)" << std::endl;
  std::cout << "\t-U     -- source id (OPTIONAL host:port)" << std::endl;
  std::cout << "\t-D     -- db name label (OPTIONAL default: db0)" << std::endl;
  std::cout << "\t-E     -- enable idempotence (true|false)" << std::endl;
  std::cout << "\t-R     -- sync protocol (auto|legacy|pb)" << std::endl;
  std::cout << "\t-H     -- heartbeat log interval seconds (OPTIONAL default: 60, min: 1, 0 disables)" << std::endl;
  std::cout << "\t-J     -- kafka producer message.max.bytes (OPTIONAL default: 1000000)" << std::endl;
  std::cout << "\t-A     -- pb ack delay warn seconds (OPTIONAL default: 10, min: 1, 0 disables)" << std::endl;
  std::cout << "\t-I     -- pb idle reconnect seconds (OPTIONAL default: 30, min: 1, 0 disables)" << std::endl;
  std::cout << "\t-F     -- event filter group or filter file (OPTIONAL, repeatable)" << std::endl;
  std::cout << "\t-X     -- global exclude key filters, comma separated (OPTIONAL)" << std::endl;
  std::cout << "\t--binlog_workers N (OPTIONAL default: 4)" << std::endl;
  std::cout << "\t--binlog_queue_size N (OPTIONAL default: 4096)" << std::endl;
  std::cout << "\t--start_from_master true|false (OPTIONAL default: false)" << std::endl;
  std::cout << "\t--snapshot_oversize_list_tail_max_items N (OPTIONAL default: 0)" << std::endl;
  std::cout << "\t--snapshot_oversize_shrink_batch true|false (OPTIONAL default: true)" << std::endl;
  std::cout << "\t--snapshot_oversize_string_policy skip|error (OPTIONAL default: skip)" << std::endl;
  std::cout << "\t--args_encoding base64|none (OPTIONAL default: base64)" << std::endl;
  std::cout << "\t--raw_resp_encoding base64|none (OPTIONAL default: base64)" << std::endl;
  std::cout << "\t--include_raw_resp true|false (OPTIONAL default: true)" << std::endl;
  std::cout << "\texample: ./pika_port -t 127.0.0.1 -p 12345 -i 127.0.0.1 -o 9221 "
               "-k 127.0.0.1:9092 -M dual -S pika.snapshot -B pika.binlog -O __pika_port_kafka_offsets "
               "-x 4 -R auto -l ./log -r ./rsync_dump -b 512 -E true"
            << std::endl;
}

static const char* KafkaStatsModeToString(KafkaStatsMode mode) {
  switch (mode) {
    case KafkaStatsMode::kNone:
      return "none";
    case KafkaStatsMode::kAggregated:
      return "agg";
    case KafkaStatsMode::kPerSender:
      return "detail";
    case KafkaStatsMode::kAll:
      return "all";
    default:
      return "unknown";
  }
}

static const char* OversizeStringPolicyToString(SnapshotOversizeStringPolicy policy) {
  switch (policy) {
    case SnapshotOversizeStringPolicy::kError:
      return "error";
    case SnapshotOversizeStringPolicy::kSkip:
      return "skip";
    default:
      return "unknown";
  }
}

static const char* PayloadEncodingToString(PayloadEncoding encoding) {
  switch (encoding) {
    case PayloadEncoding::kBase64:
      return "base64";
    case PayloadEncoding::kNone:
      return "none";
    default:
      return "none";
  }
}

static bool ParseKafkaStatsMode(const std::string& value, KafkaStatsMode* mode) {
  std::string lower = value;
  for (char& ch : lower) {
    ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
  }
  if (lower == "none") {
    *mode = KafkaStatsMode::kNone;
    return true;
  }
  if (lower == "agg" || lower == "aggregate" || lower == "aggregated") {
    *mode = KafkaStatsMode::kAggregated;
    return true;
  }
  if (lower == "detail" || lower == "details" || lower == "sender" || lower == "per-sender") {
    *mode = KafkaStatsMode::kPerSender;
    return true;
  }
  if (lower == "all") {
    *mode = KafkaStatsMode::kAll;
    return true;
  }
  return false;
}

static bool ParseBoolOption(const std::string& value, bool* out) {
  std::string lower = value;
  for (char& ch : lower) {
    ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
  }
  if (lower == "1" || lower == "true" || lower == "yes" || lower == "y") {
    *out = true;
    return true;
  }
  if (lower == "0" || lower == "false" || lower == "no" || lower == "n") {
    *out = false;
    return true;
  }
  return false;
}

static bool ParseOversizeStringPolicy(const std::string& value, SnapshotOversizeStringPolicy* policy) {
  std::string lower = value;
  for (char& ch : lower) {
    ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
  }
  if (lower == "skip") {
    *policy = SnapshotOversizeStringPolicy::kSkip;
    return true;
  }
  if (lower == "error") {
    *policy = SnapshotOversizeStringPolicy::kError;
    return true;
  }
  return false;
}

static bool ParsePayloadEncoding(const std::string& value, PayloadEncoding* encoding) {
  std::string lower = value;
  for (char& ch : lower) {
    ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
  }
  if (lower == "base64" || lower == "b64") {
    *encoding = PayloadEncoding::kBase64;
    return true;
  }
  if (lower == "none" || lower == "raw") {
    *encoding = PayloadEncoding::kNone;
    return true;
  }
  return false;
}

static std::string TrimString(std::string_view value) {
  size_t start = 0;
  while (start < value.size() && std::isspace(static_cast<unsigned char>(value[start])) != 0) {
    ++start;
  }
  size_t end = value.size();
  while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return std::string(value.substr(start, end - start));
}

static std::string ToLower(std::string_view value) {
  std::string out;
  out.reserve(value.size());
  for (char ch : value) {
    out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
  }
  return out;
}

struct ConfigFileResult {
  bool filenum_specified{false};
  bool offset_specified{false};
  std::vector<std::string> filter_group_args;
  std::vector<std::string> filter_exclude_args;
};

static bool ParseInt64(const std::string& value, int64_t* out) {
  if (!out) {
    return false;
  }
  errno = 0;
  char* end = nullptr;
  long long num = std::strtoll(value.c_str(), &end, 10);
  if (errno == ERANGE || end == value.c_str() || *end != '\0') {
    return false;
  }
  *out = static_cast<int64_t>(num);
  return true;
}

static void NormalizePath(std::string* path) {
  if (!path) {
    return;
  }
  if (!path->empty() && path->back() != '/') {
    path->push_back('/');
  }
}

static bool ApplyConfigValue(const std::string& key,
                             const std::string& value,
                             ConfigFileResult* result,
                             std::vector<std::string>* warnings) {
  std::string lower = ToLower(key);
  int64_t num = 0;
  if (lower == "filter") {
    if (result) {
      result->filter_group_args.push_back(value);
    }
    return true;
  }
  if (lower == "exclude" || lower == "exclude_keys") {
    if (result) {
      result->filter_exclude_args.push_back(value);
    }
    return true;
  }

  if (lower == "local_ip") {
    g_conf.local_ip = value;
    return true;
  }
  if (lower == "local_port") {
    if (!ParseInt64(value, &num) || num < 0) {
      return false;
    }
    g_conf.local_port = static_cast<int>(num);
    return true;
  }
  if (lower == "master_ip") {
    g_conf.master_ip = value;
    return true;
  }
  if (lower == "master_port") {
    if (!ParseInt64(value, &num) || num < 0) {
      return false;
    }
    g_conf.master_port = static_cast<int>(num);
    return true;
  }
  if (lower == "passwd" || lower == "password" || lower == "master_password") {
    g_conf.passwd = value;
    return true;
  }
  if (lower == "filenum") {
    if (!ParseInt64(value, &num) || num < 0) {
      return false;
    }
    g_conf.filenum = static_cast<size_t>(num);
    if (result) {
      result->filenum_specified = true;
    }
    return true;
  }
  if (lower == "offset") {
    if (!ParseInt64(value, &num) || num < 0) {
      return false;
    }
    g_conf.offset = static_cast<size_t>(num);
    if (result) {
      result->offset_specified = true;
    }
    return true;
  }
  if (lower == "log_path") {
    g_conf.log_path = value;
    NormalizePath(&g_conf.log_path);
    return true;
  }
  if (lower == "dump_path") {
    g_conf.dump_path = value;
    NormalizePath(&g_conf.dump_path);
    return true;
  }
  if (lower == "sync_batch_num") {
    if (!ParseInt64(value, &num) || num <= 0) {
      return false;
    }
    g_conf.sync_batch_num = static_cast<size_t>(num);
    return true;
  }
  if (lower == "wait_bgsave_timeout") {
    if (!ParseInt64(value, &num) || num <= 0) {
      return false;
    }
    g_conf.wait_bgsave_timeout = static_cast<time_t>(num);
    return true;
  }
  if (lower == "wait_bgsave_timeout_sec") {
    if (!ParseInt64(value, &num) || num <= 0) {
      return false;
    }
    g_conf.wait_bgsave_timeout = static_cast<time_t>(num);
    return true;
  }
  if (lower == "exit_if_dbsync") {
    bool enabled = false;
    if (!ParseBoolOption(value, &enabled)) {
      return false;
    }
    g_conf.exit_if_dbsync = enabled;
    return true;
  }
  if (lower == "kafka_brokers") {
    g_conf.kafka_brokers = value;
    return true;
  }
  if (lower == "kafka_client_id") {
    g_conf.kafka_client_id = value;
    return true;
  }
  if (lower == "kafka_topic_snapshot") {
    g_conf.kafka_topic_snapshot = value;
    return true;
  }
  if (lower == "kafka_topic_binlog") {
    g_conf.kafka_topic_binlog = value;
    return true;
  }
  if (lower == "kafka_topic_single") {
    g_conf.kafka_topic_single = value;
    return true;
  }
  if (lower == "kafka_topic_offsets") {
    g_conf.kafka_topic_offsets = value;
    return true;
  }
  if (lower == "kafka_offsets_enabled") {
    bool enabled = false;
    if (!ParseBoolOption(value, &enabled)) {
      return false;
    }
    g_conf.kafka_offsets_enabled = enabled;
    return true;
  }
  if (lower == "checkpoint_path") {
    g_conf.checkpoint_path = value;
    return true;
  }
  if (lower == "source_id") {
    g_conf.source_id = value;
    return true;
  }
  if (lower == "db_name") {
    g_conf.db_name = value;
    return true;
  }
  if (lower == "kafka_enable_idempotence") {
    bool enabled = false;
    if (!ParseBoolOption(value, &enabled)) {
      return false;
    }
    g_conf.kafka_enable_idempotence = enabled;
    return true;
  }
  if (lower == "kafka_stream_mode") {
    g_conf.kafka_stream_mode = value;
    return true;
  }
  if (lower == "sync_protocol") {
    g_conf.sync_protocol = value;
    return true;
  }
  if (lower == "heartbeat_interval") {
    if (!ParseInt64(value, &num)) {
      return false;
    }
    if (num <= 0) {
      g_conf.heartbeat_interval_ms = 0;
    } else {
      g_conf.heartbeat_interval_ms = static_cast<int64_t>(num) * 1000;
    }
    return true;
  }
  if (lower == "heartbeat_interval_sec") {
    if (!ParseInt64(value, &num)) {
      return false;
    }
    if (num <= 0) {
      g_conf.heartbeat_interval_ms = 0;
    } else {
      g_conf.heartbeat_interval_ms = static_cast<int64_t>(num) * 1000;
    }
    return true;
  }
  if (lower == "kafka_message_max_bytes") {
    if (!ParseInt64(value, &num) || num <= 0) {
      return false;
    }
    g_conf.kafka_message_max_bytes = static_cast<int64_t>(num);
    return true;
  }
  if (lower == "kafka_sender_threads") {
    if (!ParseInt64(value, &num) || num <= 0) {
      return false;
    }
    g_conf.kafka_sender_threads = static_cast<int>(num);
    return true;
  }
  if (lower == "kafka_stats_mode") {
    KafkaStatsMode mode;
    if (!ParseKafkaStatsMode(value, &mode)) {
      return false;
    }
    g_conf.kafka_stats_mode = mode;
    return true;
  }
  if (lower == "binlog_workers") {
    if (!ParseInt64(value, &num) || num <= 0) {
      return false;
    }
    g_conf.binlog_workers = static_cast<int>(num);
    return true;
  }
  if (lower == "binlog_queue_size") {
    if (!ParseInt64(value, &num) || num <= 0) {
      return false;
    }
    g_conf.binlog_queue_size = static_cast<size_t>(num);
    return true;
  }
  if (lower == "pb_ack_delay_warn") {
    if (!ParseInt64(value, &num)) {
      return false;
    }
    if (num <= 0) {
      g_conf.pb_ack_delay_warn_ms = 0;
    } else {
      g_conf.pb_ack_delay_warn_ms = static_cast<int64_t>(num) * 1000;
    }
    return true;
  }
  if (lower == "pb_ack_delay_warn_sec") {
    if (!ParseInt64(value, &num)) {
      return false;
    }
    if (num <= 0) {
      g_conf.pb_ack_delay_warn_ms = 0;
    } else {
      g_conf.pb_ack_delay_warn_ms = static_cast<int64_t>(num) * 1000;
    }
    return true;
  }
  if (lower == "pb_idle_timeout") {
    if (!ParseInt64(value, &num)) {
      return false;
    }
    if (num <= 0) {
      g_conf.pb_idle_timeout_ms = 0;
    } else {
      g_conf.pb_idle_timeout_ms = static_cast<int64_t>(num) * 1000;
    }
    return true;
  }
  if (lower == "pb_idle_timeout_sec") {
    if (!ParseInt64(value, &num)) {
      return false;
    }
    if (num <= 0) {
      g_conf.pb_idle_timeout_ms = 0;
    } else {
      g_conf.pb_idle_timeout_ms = static_cast<int64_t>(num) * 1000;
    }
    return true;
  }
  if (lower == "snapshot_oversize_list_tail_max_items") {
    if (!ParseInt64(value, &num) || num < 0) {
      return false;
    }
    g_conf.snapshot_oversize_list_tail_max_items = static_cast<size_t>(num);
    return true;
  }
  if (lower == "snapshot_oversize_shrink_batch") {
    bool enabled = false;
    if (!ParseBoolOption(value, &enabled)) {
      return false;
    }
    g_conf.snapshot_oversize_shrink_batch = enabled;
    return true;
  }
  if (lower == "snapshot_oversize_string_policy") {
    SnapshotOversizeStringPolicy policy;
    if (!ParseOversizeStringPolicy(value, &policy)) {
      return false;
    }
    g_conf.snapshot_oversize_string_policy = policy;
    return true;
  }
  if (lower == "args_encoding") {
    PayloadEncoding encoding;
    if (!ParsePayloadEncoding(value, &encoding)) {
      return false;
    }
    g_conf.args_encoding = encoding;
    return true;
  }
  if (lower == "raw_resp_encoding") {
    PayloadEncoding encoding;
    if (!ParsePayloadEncoding(value, &encoding)) {
      return false;
    }
    g_conf.raw_resp_encoding = encoding;
    return true;
  }
  if (lower == "include_raw_resp") {
    bool enabled = false;
    if (!ParseBoolOption(value, &enabled)) {
      return false;
    }
    g_conf.include_raw_resp = enabled;
    return true;
  }
  if (lower == "start_from_master") {
    bool enabled = false;
    if (!ParseBoolOption(value, &enabled)) {
      return false;
    }
    g_conf.start_from_master = enabled;
    return true;
  }
  if (lower == "vlog_level") {
    if (ParseInt64(value, &num)) {
      FLAGS_v = static_cast<int>(num);
    } else if (warnings) {
      warnings->push_back("Invalid vlog_level: " + value);
    }
    return true;
  }

  if (warnings) {
    warnings->push_back("Unknown config key: " + key);
  }
  return true;
}

static bool LoadConfigFile(const std::string& path,
                           ConfigFileResult* result,
                           std::vector<std::string>* warnings) {
  if (path.empty()) {
    return true;
  }
  std::ifstream in(path);
  if (!in.is_open()) {
    if (warnings) {
      warnings->push_back("config file not found: " + path);
    }
    return false;
  }
  std::string line;
  int lineno = 0;
  while (std::getline(in, line)) {
    lineno++;
    std::string trimmed = TrimString(line);
    if (trimmed.empty() || trimmed[0] == '#' || trimmed[0] == ';') {
      continue;
    }
    size_t pos = trimmed.find('=');
    if (pos == std::string::npos) {
      if (warnings) {
        warnings->push_back("config parse error line " + std::to_string(lineno) + ": missing '='");
      }
      continue;
    }
    std::string key = TrimString(trimmed.substr(0, pos));
    std::string value = TrimString(trimmed.substr(pos + 1));
    if (key.empty()) {
      if (warnings) {
        warnings->push_back("config parse error line " + std::to_string(lineno) + ": empty key");
      }
      continue;
    }
    if (!ApplyConfigValue(key, value, result, warnings)) {
      if (warnings) {
        warnings->push_back("config parse error line " + std::to_string(lineno) + ": " + key);
      }
    }
  }
  return true;
}

static bool ExtractConfigPath(int argc, char* argv[], std::string* path, std::string* err) {
  if (!path) {
    return false;
  }
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "-C" || arg == "--config") {
      if (i + 1 >= argc) {
        if (err) {
          *err = "missing value for --config";
        }
        return false;
      }
      *path = argv[i + 1];
      return true;
    }
    const std::string prefix = "--config=";
    if (arg.compare(0, prefix.size(), prefix) == 0) {
      *path = arg.substr(prefix.size());
      return true;
    }
  }
  return true;
}

static void LoadFilterFile(const std::string& path,
                           std::vector<std::string>* groups,
                           std::vector<std::string>* warnings) {
  if (!groups || !warnings) {
    return;
  }
  std::ifstream in(path);
  if (!in.is_open()) {
    warnings->push_back("filter file not found: " + path);
    return;
  }
  std::string line;
  while (std::getline(in, line)) {
    std::string trimmed = TrimString(line);
    if (trimmed.empty() || trimmed[0] == '#') {
      continue;
    }
    groups->push_back(trimmed);
  }
}

static void ExpandFilterGroupArgs(const std::vector<std::string>& args,
                                  std::vector<std::string>* groups,
                                  std::vector<std::string>* warnings) {
  if (!groups || !warnings) {
    return;
  }
  for (const auto& arg : args) {
    std::string trimmed = TrimString(arg);
    if (trimmed.empty()) {
      continue;
    }
    if (trimmed.find('=') != std::string::npos) {
      groups->push_back(trimmed);
      continue;
    }
    LoadFilterFile(trimmed, groups, warnings);
  }
}

void PrintInfo(const std::time_t& now) {
  std::cout << "================== Pika Port Kafka ==================" << std::endl;
  std::cout << "local_ip:" << g_conf.local_ip << std::endl;
  std::cout << "Local_port:" << g_conf.local_port << std::endl;
  std::cout << "Master_ip:" << g_conf.master_ip << std::endl;
  std::cout << "Master_port:" << g_conf.master_port << std::endl;
  std::cout << "Kafka_sender_threads:" << g_conf.kafka_sender_threads << std::endl;
  std::cout << "Kafka_stats_mode:" << KafkaStatsModeToString(g_conf.kafka_stats_mode) << std::endl;
  std::cout << "Wait_bgsave_timeout:" << g_conf.wait_bgsave_timeout << std::endl;
  std::cout << "Log_path:" << g_conf.log_path << std::endl;
  std::cout << "Dump_path:" << g_conf.dump_path << std::endl;
  std::cout << "Filenum:" << g_conf.filenum << std::endl;
  std::cout << "Offset:" << g_conf.offset << std::endl;
  std::cout << "Passwd:" << g_conf.passwd << std::endl;
  std::cout << "Sync_batch_num:" << g_conf.sync_batch_num << std::endl;
  std::cout << "Kafka_brokers:" << g_conf.kafka_brokers << std::endl;
  std::cout << "Kafka_client_id:" << g_conf.kafka_client_id << std::endl;
  std::cout << "Kafka_stream_mode:" << g_conf.kafka_stream_mode << std::endl;
  std::cout << "Kafka_topic_snapshot:" << g_conf.kafka_topic_snapshot << std::endl;
  std::cout << "Kafka_topic_binlog:" << g_conf.kafka_topic_binlog << std::endl;
  std::cout << "Kafka_topic_single:" << g_conf.kafka_topic_single << std::endl;
  std::cout << "Kafka_topic_offsets:" << g_conf.kafka_topic_offsets << std::endl;
  std::cout << "Kafka_offsets_enabled:" << (g_conf.kafka_offsets_enabled ? "true" : "false") << std::endl;
  std::cout << "Checkpoint_path:" << g_conf.checkpoint_path << std::endl;
  std::cout << "Source_id:" << g_conf.source_id << std::endl;
  std::cout << "Db_name:" << g_conf.db_name << std::endl;
  std::cout << "Kafka_enable_idempotence:" << (g_conf.kafka_enable_idempotence ? "true" : "false") << std::endl;
  std::cout << "Sync_protocol:" << g_conf.sync_protocol << std::endl;
  std::cout << "Heartbeat_interval_ms:" << g_conf.heartbeat_interval_ms << std::endl;
  std::cout << "Kafka_message_max_bytes:" << g_conf.kafka_message_max_bytes << std::endl;
  std::cout << "Pb_ack_delay_warn_ms:" << g_conf.pb_ack_delay_warn_ms << std::endl;
  std::cout << "Pb_idle_timeout_ms:" << g_conf.pb_idle_timeout_ms << std::endl;
  std::cout << "Binlog_workers:" << g_conf.binlog_workers << std::endl;
  std::cout << "Binlog_queue_size:" << g_conf.binlog_queue_size << std::endl;
  std::cout << "Snapshot_oversize_list_tail_max_items:" << g_conf.snapshot_oversize_list_tail_max_items << std::endl;
  std::cout << "Snapshot_oversize_shrink_batch:" << (g_conf.snapshot_oversize_shrink_batch ? "true" : "false")
            << std::endl;
  std::cout << "Snapshot_oversize_string_policy:" << OversizeStringPolicyToString(g_conf.snapshot_oversize_string_policy)
            << std::endl;
  std::cout << "Args_encoding:" << PayloadEncodingToString(g_conf.args_encoding) << std::endl;
  std::cout << "Raw_resp_encoding:" << PayloadEncodingToString(g_conf.raw_resp_encoding) << std::endl;
  std::cout << "Include_raw_resp:" << (g_conf.include_raw_resp ? "true" : "false") << std::endl;
  std::cout << "Start_from_master:" << (g_conf.start_from_master ? "true" : "false") << std::endl;
  if (g_conf.event_filter) {
    g_conf.event_filter->Dump(std::cout);
  } else {
    std::cout << "Event_filter_groups:0" << std::endl;
    std::cout << "Event_filter_exclude:none" << std::endl;
  }
  std::cout << "Startup Time : " << asctime(localtime(&now));
  std::cout << "========================================================" << std::endl;
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    Usage();
    exit(-1);
  }

  int c;
  char buf[1024];
  bool is_daemon = false;
  long num = 0;
  bool filenum_specified = false;
  bool offset_specified = false;
  std::string config_path;
  std::string config_err;
  ConfigFileResult config_result;
  std::vector<std::string> config_warnings;
  std::vector<std::string> filter_group_args;
  std::vector<std::string> filter_exclude_args;
  enum LongOptionValues {
    kLongSnapshotOversizeListTailMaxItems = 1000,
    kLongSnapshotOversizeShrinkBatch,
    kLongSnapshotOversizeStringPolicy,
    kLongArgsEncoding,
    kLongRawRespEncoding,
    kLongIncludeRawResp,
    kLongBinlogWorkers,
    kLongBinlogQueueSize,
    kLongStartFromMaster,
  };
  static struct option long_options[] = {
      {"config", required_argument, nullptr, 'C'},
      {"snapshot_oversize_list_tail_max_items", required_argument, nullptr, kLongSnapshotOversizeListTailMaxItems},
      {"snapshot_oversize_shrink_batch", required_argument, nullptr, kLongSnapshotOversizeShrinkBatch},
      {"snapshot_oversize_string_policy", required_argument, nullptr, kLongSnapshotOversizeStringPolicy},
      {"args_encoding", required_argument, nullptr, kLongArgsEncoding},
      {"raw_resp_encoding", required_argument, nullptr, kLongRawRespEncoding},
      {"include_raw_resp", required_argument, nullptr, kLongIncludeRawResp},
      {"binlog_workers", required_argument, nullptr, kLongBinlogWorkers},
      {"binlog_queue_size", required_argument, nullptr, kLongBinlogQueueSize},
      {"start_from_master", required_argument, nullptr, kLongStartFromMaster},
      {nullptr, 0, nullptr, 0},
  };

  if (!ExtractConfigPath(argc, argv, &config_path, &config_err)) {
    if (!config_err.empty()) {
      fprintf(stderr, "%s\n", config_err.c_str());
    }
    Usage();
    return -1;
  }
  if (!config_path.empty()) {
    if (!LoadConfigFile(config_path, &config_result, &config_warnings)) {
      for (const auto& warning : config_warnings) {
        fprintf(stderr, "Config: %s\n", warning.c_str());
      }
      fprintf(stderr, "Failed to load config file: %s\n", config_path.c_str());
      return -1;
    }
  }
  filenum_specified = config_result.filenum_specified;
  offset_specified = config_result.offset_specified;
  filter_group_args = config_result.filter_group_args;
  filter_exclude_args = config_result.filter_exclude_args;
  while (-1 != (c = getopt_long(argc, argv,
                               "C:t:p:i:o:f:s:w:r:l:x:G:z:b:H:J:A:I:F:X:edhk:c:S:B:T:O:P:M:U:D:E:R:Q:",
                               long_options, nullptr))) {
    switch (c) {
      case 'C':
        snprintf(buf, 1024, "%s", optarg);
        config_path = std::string(buf);
        break;
      case 't':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.local_ip = std::string(buf);
        break;
      case 'p':
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        g_conf.local_port = static_cast<int>(num);
        break;
      case 'i':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.master_ip = std::string(buf);
        break;
      case 'o':
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        g_conf.master_port = static_cast<int>(num);
        break;
      case 'x':
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        g_conf.kafka_sender_threads = static_cast<int>(num);
        break;
      case 'G': {
        snprintf(buf, 1024, "%s", optarg);
        KafkaStatsMode mode;
        if (!ParseKafkaStatsMode(std::string(buf), &mode)) {
          fprintf(stderr, "Invalid kafka stats mode: %s\n", buf);
          Usage();
          exit(-1);
        }
        g_conf.kafka_stats_mode = mode;
        break;
      }
      case 'z':
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        g_conf.wait_bgsave_timeout = static_cast<time_t>(num);
        break;

      case 'f':
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        g_conf.filenum = static_cast<size_t>(num);
        filenum_specified = true;
        break;
      case 's':
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        g_conf.offset = static_cast<size_t>(num);
        offset_specified = true;
        break;
      case 'w':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.passwd = std::string(buf);
        break;

      case 'r':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.dump_path = std::string(buf);
        if (g_conf.dump_path[g_conf.dump_path.length() - 1] != '/') {
          g_conf.dump_path.append("/");
        }
        break;
      case 'l':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.log_path = std::string(buf);
        if (g_conf.log_path[g_conf.log_path.length() - 1] != '/') {
          g_conf.log_path.append("/");
        }
        break;
      case 'b':
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        g_conf.sync_batch_num = static_cast<size_t>(num);
        break;
      case 'k':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.kafka_brokers = std::string(buf);
        break;
      case 'c':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.kafka_client_id = std::string(buf);
        break;
      case 'S':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.kafka_topic_snapshot = std::string(buf);
        break;
      case 'B':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.kafka_topic_binlog = std::string(buf);
        break;
      case 'T':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.kafka_topic_single = std::string(buf);
        break;
      case 'O':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.kafka_topic_offsets = std::string(buf);
        break;
      case 'Q':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.kafka_offsets_enabled = (std::string(buf) != "false");
        break;
      case 'P':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.checkpoint_path = std::string(buf);
        break;
      case 'M':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.kafka_stream_mode = std::string(buf);
        break;
      case 'U':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.source_id = std::string(buf);
        break;
      case 'D':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.db_name = std::string(buf);
        break;
      case 'E':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.kafka_enable_idempotence = (std::string(buf) == "true");
        break;
      case 'R':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.sync_protocol = std::string(buf);
        break;
      case 'H':
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        if (num <= 0) {
          g_conf.heartbeat_interval_ms = 0;
        } else {
          g_conf.heartbeat_interval_ms = static_cast<int64_t>(num) * 1000;
        }
        break;
      case 'J':
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        if (num > 0) {
          g_conf.kafka_message_max_bytes = static_cast<int64_t>(num);
        }
        break;
      case 'A':
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        if (num <= 0) {
          g_conf.pb_ack_delay_warn_ms = 0;
        } else {
          g_conf.pb_ack_delay_warn_ms = static_cast<int64_t>(num) * 1000;
        }
        break;
      case 'I':
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        if (num <= 0) {
          g_conf.pb_idle_timeout_ms = 0;
        } else {
          g_conf.pb_idle_timeout_ms = static_cast<int64_t>(num) * 1000;
        }
        break;
      case 'F':
        snprintf(buf, 1024, "%s", optarg);
        filter_group_args.emplace_back(buf);
        break;
      case 'X':
        snprintf(buf, 1024, "%s", optarg);
        filter_exclude_args.emplace_back(buf);
        break;
      case kLongSnapshotOversizeListTailMaxItems:
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        if (num < 0) {
          fprintf(stderr, "Invalid snapshot_oversize_list_tail_max_items: %s\n", buf);
          Usage();
          exit(-1);
        }
        g_conf.snapshot_oversize_list_tail_max_items = static_cast<size_t>(num);
        break;
      case kLongSnapshotOversizeShrinkBatch: {
        snprintf(buf, 1024, "%s", optarg);
        bool enabled = false;
        if (!ParseBoolOption(std::string(buf), &enabled)) {
          fprintf(stderr, "Invalid snapshot_oversize_shrink_batch: %s\n", buf);
          Usage();
          exit(-1);
        }
        g_conf.snapshot_oversize_shrink_batch = enabled;
        break;
      }
      case kLongSnapshotOversizeStringPolicy: {
        snprintf(buf, 1024, "%s", optarg);
        SnapshotOversizeStringPolicy policy;
        if (!ParseOversizeStringPolicy(std::string(buf), &policy)) {
          fprintf(stderr, "Invalid snapshot_oversize_string_policy: %s\n", buf);
          Usage();
          exit(-1);
        }
        g_conf.snapshot_oversize_string_policy = policy;
        break;
      }
      case kLongArgsEncoding: {
        snprintf(buf, 1024, "%s", optarg);
        PayloadEncoding encoding;
        if (!ParsePayloadEncoding(std::string(buf), &encoding)) {
          fprintf(stderr, "Invalid args_encoding: %s\n", buf);
          Usage();
          exit(-1);
        }
        g_conf.args_encoding = encoding;
        break;
      }
      case kLongRawRespEncoding: {
        snprintf(buf, 1024, "%s", optarg);
        PayloadEncoding encoding;
        if (!ParsePayloadEncoding(std::string(buf), &encoding)) {
          fprintf(stderr, "Invalid raw_resp_encoding: %s\n", buf);
          Usage();
          exit(-1);
        }
        g_conf.raw_resp_encoding = encoding;
        break;
      }
      case kLongIncludeRawResp: {
        snprintf(buf, 1024, "%s", optarg);
        bool enabled = false;
        if (!ParseBoolOption(std::string(buf), &enabled)) {
          fprintf(stderr, "Invalid include_raw_resp: %s\n", buf);
          Usage();
          exit(-1);
        }
        g_conf.include_raw_resp = enabled;
        break;
      }
      case kLongBinlogWorkers:
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        if (num > 0) {
          g_conf.binlog_workers = static_cast<int>(num);
        }
        break;
      case kLongBinlogQueueSize:
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        if (num > 0) {
          g_conf.binlog_queue_size = static_cast<size_t>(num);
        }
        break;
      case kLongStartFromMaster: {
        snprintf(buf, 1024, "%s", optarg);
        bool enabled = false;
        if (!ParseBoolOption(std::string(buf), &enabled)) {
          fprintf(stderr, "Invalid start_from_master: %s\n", buf);
          Usage();
          exit(-1);
        }
        g_conf.start_from_master = enabled;
        break;
      }
      case 'e':
        g_conf.exit_if_dbsync = true;
        break;
      case 'd':
        is_daemon = true;
        break;
      case 'h':
        Usage();
        return 0;
      default:
        Usage();
        return 0;
    }
  }

  std::vector<std::string> filter_group_specs;
  std::vector<std::string> filter_warnings;
  ExpandFilterGroupArgs(filter_group_args, &filter_group_specs, &filter_warnings);

  const int64_t kMinHeartbeatIntervalMs = 1000;
  const int64_t kMinAckDelayWarnMs = 1000;
  const int64_t kMinIdleTimeoutMs = 1000;
  bool heartbeat_clamped = false;
  bool ack_delay_clamped = false;
  bool idle_timeout_clamped = false;
  if (g_conf.heartbeat_interval_ms > 0 && g_conf.heartbeat_interval_ms < kMinHeartbeatIntervalMs) {
    g_conf.heartbeat_interval_ms = kMinHeartbeatIntervalMs;
    heartbeat_clamped = true;
  }
  if (g_conf.pb_ack_delay_warn_ms > 0 && g_conf.pb_ack_delay_warn_ms < kMinAckDelayWarnMs) {
    g_conf.pb_ack_delay_warn_ms = kMinAckDelayWarnMs;
    ack_delay_clamped = true;
  }
  if (g_conf.pb_idle_timeout_ms > 0 && g_conf.pb_idle_timeout_ms < kMinIdleTimeoutMs) {
    g_conf.pb_idle_timeout_ms = kMinIdleTimeoutMs;
    idle_timeout_clamped = true;
  }

  GlogInit(g_conf.log_path, is_daemon);
  for (const auto& warning : config_warnings) {
    LOG(WARNING) << "Config: " << warning;
  }
  if (heartbeat_clamped) {
    LOG(WARNING) << "Heartbeat interval too small, set to " << g_conf.heartbeat_interval_ms << "ms";
  }
  if (ack_delay_clamped) {
    LOG(WARNING) << "PB ack delay warn interval too small, set to " << g_conf.pb_ack_delay_warn_ms << "ms";
  }
  if (idle_timeout_clamped) {
    LOG(WARNING) << "PB idle reconnect interval too small, set to " << g_conf.pb_idle_timeout_ms << "ms";
  }
  g_conf.event_filter = EventFilter::Build(filter_group_specs, filter_exclude_args, &filter_warnings);
  for (const auto& warning : filter_warnings) {
    LOG(WARNING) << "Filter config: " << warning;
  }
  if (g_conf.source_id.empty()) {
    g_conf.source_id = g_conf.master_ip + ":" + std::to_string(g_conf.master_port);
  }
  if (g_conf.kafka_stream_mode != "dual" && g_conf.kafka_stream_mode != "single") {
    g_conf.kafka_stream_mode = "dual";
  }
  if (g_conf.sync_protocol != "auto" && g_conf.sync_protocol != "legacy" && g_conf.sync_protocol != "pb") {
    g_conf.sync_protocol = "auto";
  }
  if (g_conf.binlog_workers <= 0) {
    g_conf.binlog_workers = 1;
  }
  if (g_conf.binlog_queue_size == 0) {
    g_conf.binlog_queue_size = 1024;
  }

  if ((filenum_specified || offset_specified) && g_conf.start_from_master) {
    LOG(WARNING) << "start_from_master ignored because filenum/offset specified";
    g_conf.start_from_master = false;
  }
  if (g_conf.start_from_master) {
    ReplicationInfo info;
    std::string err;
    if (!FetchReplicationInfo(g_conf.master_ip, g_conf.master_port, g_conf.passwd, &info, &err) ||
        !info.has_binlog_offset) {
      LOG(ERROR) << "Failed to fetch master binlog_offset: " << err;
      return -1;
    }
    g_conf.filenum = info.filenum;
    g_conf.offset = info.offset;
    filenum_specified = true;
    offset_specified = true;
    Checkpoint cp;
    cp.filenum = info.filenum;
    cp.offset = info.offset;
    cp.logic_id = 0;
    cp.server_id = 0;
    cp.term_id = 0;
    cp.ts_ms = 0;
    CheckpointManager checkpoint_manager(g_conf.checkpoint_path, g_conf.source_id,
                                         g_conf.kafka_topic_offsets, g_conf.kafka_brokers,
                                         g_conf.kafka_offsets_enabled);
    checkpoint_manager.OnFiltered(cp);
    checkpoint_manager.FlushFiltered();
    LOG(INFO) << "Start from master binlog_offset=" << info.filenum << ":" << info.offset;
  }

  if (g_conf.filenum == UINT32_MAX) {
    CheckpointManager checkpoint_manager(g_conf.checkpoint_path, g_conf.source_id,
                                         g_conf.kafka_topic_offsets, g_conf.kafka_brokers,
                                         g_conf.kafka_offsets_enabled);
    Checkpoint cp;
    if (checkpoint_manager.Load(&cp)) {
      g_conf.filenum = cp.filenum;
      g_conf.offset = cp.offset;
      LOG(INFO) << "Loaded checkpoint filenum=" << cp.filenum << " offset=" << cp.offset;
    }
  }
  if ((filenum_specified || offset_specified) && g_conf.filenum == 0 && g_conf.offset == 0) {
    LOG(WARNING) << "Start offset forced to 0: full sync (DBSync/rsync) may be triggered";
  }

  if (g_conf.local_port == 0) {
    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<int> di(10000, 40000);
    // g_conf.local_port = di(mt);
    g_conf.local_port = 21333;
    LOG(INFO) << "Use random port: " << g_conf.local_port;
  }

  std::chrono::system_clock::time_point start_time = std::chrono::system_clock::now();
  std::time_t now = std::chrono::system_clock::to_time_t(start_time);
  PrintInfo(now);

  if (g_conf.master_port == 0 || g_conf.kafka_brokers.empty() || g_conf.sync_batch_num == 0 ||
      g_conf.wait_bgsave_timeout <= 0) {
    fprintf(stderr, "Invalid Arguments\n");
    Usage();
    exit(-1);
  }

  if (g_conf.kafka_stream_mode == "single") {
    if (g_conf.kafka_topic_single.empty()) {
      fprintf(stderr, "Invalid Arguments: kafka_topic_single required in single mode\n");
      Usage();
      exit(-1);
    }
  } else {
    if (g_conf.kafka_topic_snapshot.empty() || g_conf.kafka_topic_binlog.empty()) {
      fprintf(stderr, "Invalid Arguments: kafka_topic_snapshot/binlog required in dual mode\n");
      Usage();
      exit(-1);
    }
  }

  std::string pid_file_name = "/tmp/pika_port_" + std::to_string(getpid());
  createPidFile(pid_file_name.c_str());

  // daemonize if needed
  if (is_daemon) {
    daemonize();
  }

  SignalSetup();

  g_pika_port = new PikaPort(g_conf.master_ip, g_conf.master_port, g_conf.passwd);
  if (is_daemon) {
    close_std();
  }

  g_pika_port->Start();

  return 0;
}
