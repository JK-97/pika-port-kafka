#include "event_builder.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <sstream>
#include <unordered_set>

#include "json_utils.h"

namespace {

std::string ToLower(std::string input) {
  std::transform(input.begin(), input.end(), input.begin(), [](unsigned char c) { return std::tolower(c); });
  return input;
}

uint64_t NowMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

void AppendJsonString(std::string* out, const std::string& key, const std::string& value, bool* first) {
  if (!*first) {
    out->append(",");
  }
  *first = false;
  out->append("\"");
  out->append(key);
  out->append("\":\"");
  out->append(JsonEscape(value));
  out->append("\"");
}

void AppendJsonNumber(std::string* out, const std::string& key, uint64_t value, bool* first) {
  if (!*first) {
    out->append(",");
  }
  *first = false;
  out->append("\"");
  out->append(key);
  out->append("\":");
  out->append(std::to_string(value));
}

std::string BuildArgsB64Json(const net::RedisCmdArgsType& argv) {
  std::string out;
  out.append("[");
  for (size_t i = 0; i < argv.size(); ++i) {
    if (i > 0) {
      out.append(",");
    }
    std::string encoded = Base64Encode(argv[i]);
    out.append("\"");
    out.append(encoded);
    out.append("\"");
  }
  out.append("]");
  return out;
}

void AppendSourceObject(std::string* out, const std::string& source_id, bool* first) {
  if (!*first) {
    out->append(",");
  }
  *first = false;
  out->append("\"source\":{");
  size_t pos = source_id.find(':');
  if (pos != std::string::npos) {
    std::string host = source_id.substr(0, pos);
    std::string port = source_id.substr(pos + 1);
    out->append("\"host\":\"");
    out->append(JsonEscape(host));
    out->append("\",\"port\":");
    out->append(port);
  } else {
    out->append("\"host\":\"");
    out->append(JsonEscape(source_id));
    out->append("\"");
  }
  out->append("}");
}

}  // namespace

std::string BuildPartitionKey(const std::string& db_name, const std::string& data_type, const std::string& key) {
  if (key.empty()) {
    return db_name + ":" + data_type;
  }
  return db_name + ":" + data_type + ":" + key;
}

std::string CommandDataType(const std::string& cmd) {
  const std::string op = ToLower(cmd);
  static const std::unordered_set<std::string> kStringOps = {
      "set", "setex", "psetex", "setnx", "append", "incr", "decr", "mset", "msetnx", "incrby",
      "decrby", "getset"};
  static const std::unordered_set<std::string> kHashOps = {
      "hset", "hmset", "hdel", "hincrby", "hincrbyfloat"};
  static const std::unordered_set<std::string> kListOps = {
      "lpush", "rpush", "lpop", "rpop", "ltrim", "lset", "linsert", "rpoplpush"};
  static const std::unordered_set<std::string> kSetOps = {
      "sadd", "srem", "spop", "sinterstore", "sunionstore", "sdiffstore"};
  static const std::unordered_set<std::string> kZsetOps = {
      "zadd", "zrem", "zincrby", "zremrangebyrank", "zremrangebyscore"};

  if (kStringOps.count(op) > 0) {
    return "string";
  }
  if (kHashOps.count(op) > 0) {
    return "hash";
  }
  if (kListOps.count(op) > 0) {
    return "list";
  }
  if (kSetOps.count(op) > 0) {
    return "set";
  }
  if (kZsetOps.count(op) > 0) {
    return "zset";
  }
  return "unknown";
}

std::string BuildSnapshotEventJson(const net::RedisCmdArgsType& argv,
                                   const std::string& db_name,
                                   const std::string& data_type,
                                   const std::string& source_id,
                                   const std::string& raw_resp,
                                   const std::string& key) {
  std::string out;
  out.reserve(512);
  bool first = true;
  out.append("{");
  AppendJsonString(&out, "event_type", "snapshot", &first);
  if (!argv.empty()) {
    AppendJsonString(&out, "op", ToLower(argv[0]), &first);
  }
  AppendJsonString(&out, "data_type", data_type, &first);
  AppendJsonString(&out, "db", db_name, &first);
  AppendJsonNumber(&out, "slot", 0, &first);
  AppendJsonString(&out, "key", key, &first);
  out.append(",\"args_b64\":");
  out.append(BuildArgsB64Json(argv));
  AppendJsonString(&out, "raw_resp_b64", Base64Encode(raw_resp), &first);
  AppendJsonNumber(&out, "ts_ms", NowMillis(), &first);
  AppendJsonString(&out, "event_id", "snapshot:" + db_name + ":" + data_type + ":" + key, &first);
  AppendJsonString(&out, "source_id", source_id, &first);
  AppendSourceObject(&out, source_id, &first);
  out.append("}");
  return out;
}

std::string BuildBinlogEventJson(const net::RedisCmdArgsType& argv,
                                 const PortBinlogItem& item,
                                 const std::string& db_name,
                                 const std::string& data_type,
                                 const std::string& source_id,
                                 const std::string& raw_resp,
                                 const std::string& key) {
  std::string out;
  out.reserve(512);
  bool first = true;
  out.append("{");
  AppendJsonString(&out, "event_type", "binlog", &first);
  if (!argv.empty()) {
    AppendJsonString(&out, "op", ToLower(argv[0]), &first);
  }
  AppendJsonString(&out, "data_type", data_type, &first);
  AppendJsonString(&out, "db", db_name, &first);
  AppendJsonNumber(&out, "slot", 0, &first);
  AppendJsonString(&out, "key", key, &first);
  out.append(",\"args_b64\":");
  out.append(BuildArgsB64Json(argv));
  AppendJsonString(&out, "raw_resp_b64", Base64Encode(raw_resp), &first);
  AppendJsonNumber(&out, "ts_ms", static_cast<uint64_t>(item.exec_time()) * 1000, &first);
  std::string event_id = std::to_string(item.server_id()) + ":" + std::to_string(item.filenum()) + ":" +
                         std::to_string(item.offset()) + ":" + std::to_string(item.logic_id());
  AppendJsonString(&out, "event_id", event_id, &first);
  AppendJsonString(&out, "source_id", source_id, &first);

  out.append(",\"binlog\":{");
  bool binlog_first = true;
  AppendJsonNumber(&out, "filenum", item.filenum(), &binlog_first);
  AppendJsonNumber(&out, "offset", item.offset(), &binlog_first);
  AppendJsonNumber(&out, "logic_id", item.logic_id(), &binlog_first);
  AppendJsonNumber(&out, "server_id", item.server_id(), &binlog_first);
  out.append("}");

  AppendSourceObject(&out, source_id, &first);
  out.append("}");
  return out;
}
