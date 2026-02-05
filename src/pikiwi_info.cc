#include "pikiwi_info.h"

#include <algorithm>
#include <cctype>
#include <memory>
#include <sstream>
#include <string_view>
#include <vector>

#include <glog/logging.h>

#include "net/include/net_cli.h"
#include "net/include/redis_cli.h"
#include "pstd/include/pstd_status.h"

namespace {

std::string Trim(std::string_view value) {
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

std::string ToLower(std::string_view value) {
  std::string out;
  out.reserve(value.size());
  for (char ch : value) {
    out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
  }
  return out;
}

std::vector<uint64_t> ExtractNumbers(std::string_view value) {
  std::vector<uint64_t> nums;
  uint64_t current = 0;
  bool in_number = false;
  for (char ch : value) {
    if (ch >= '0' && ch <= '9') {
      current = current * 10 + static_cast<uint64_t>(ch - '0');
      in_number = true;
    } else if (in_number) {
      nums.push_back(current);
      current = 0;
      in_number = false;
    }
  }
  if (in_number) {
    nums.push_back(current);
  }
  return nums;
}

bool SendCommand(net::NetCli* cli, const net::RedisCmdArgsType& argv, std::string* out) {
  if (!cli) {
    return false;
  }
  std::string cmd;
  net::SerializeRedisCommand(argv, &cmd);
  pstd::Status s = cli->Send(&cmd);
  if (!s.ok()) {
    return false;
  }
  net::RedisCmdArgsType reply;
  s = cli->Recv(&reply);
  if (!s.ok()) {
    return false;
  }
  if (reply.empty()) {
    return false;
  }
  if (out) {
    *out = reply[0];
  }
  return true;
}

}  // namespace

bool ParseReplicationInfo(const std::string& info, ReplicationInfo* out, std::string* err) {
  if (!out) {
    return false;
  }
  ReplicationInfo parsed;
  bool has_filenum = false;
  uint32_t filenum = 0;
  std::istringstream iss(info);
  std::string line;
  while (std::getline(iss, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    std::string trimmed = Trim(line);
    if (trimmed.empty() || trimmed[0] == '#') {
      continue;
    }
    std::string lower_line = ToLower(trimmed);
    size_t binlog_pos = lower_line.find("binlog_offset=");
    if (binlog_pos != std::string::npos) {
      std::string_view slice(trimmed);
      slice.remove_prefix(binlog_pos + std::string("binlog_offset=").size());
      auto nums = ExtractNumbers(slice);
      if (nums.size() >= 2) {
        parsed.filenum = static_cast<uint32_t>(nums[0]);
        parsed.offset = nums[1];
        parsed.has_binlog_offset = true;
      } else if (nums.size() == 1) {
        parsed.offset = nums[0];
        parsed.filenum = has_filenum ? filenum : 0;
        parsed.has_binlog_offset = true;
      }
    }
    size_t lag_pos = lower_line.find("lag=(");
    if (lag_pos != std::string::npos) {
      std::string_view lag_slice(trimmed);
      lag_slice.remove_prefix(lag_pos + std::string("lag=(").size());
      auto nums = ExtractNumbers(lag_slice);
      if (!nums.empty()) {
        int64_t lag = static_cast<int64_t>(*std::max_element(nums.begin(), nums.end()));
        if (!parsed.has_master_lag || lag > parsed.master_lag) {
          parsed.master_lag = lag;
          parsed.has_master_lag = true;
        }
      }
    }
    size_t pos = trimmed.find(':');
    if (pos == std::string::npos) {
      continue;
    }
    std::string key = ToLower(Trim(trimmed.substr(0, pos)));
    std::string value = Trim(trimmed.substr(pos + 1));
    if (key == "binlog_offset") {
      auto nums = ExtractNumbers(value);
      if (nums.size() >= 2) {
        parsed.filenum = static_cast<uint32_t>(nums[0]);
        parsed.offset = nums[1];
        parsed.has_binlog_offset = true;
      } else if (nums.size() == 1) {
        parsed.offset = nums[0];
        parsed.filenum = has_filenum ? filenum : 0;
        parsed.has_binlog_offset = true;
      }
    } else if (key == "binlog_file_num" || key == "binlog_filenum" || key == "binlog_file") {
      auto nums = ExtractNumbers(value);
      if (!nums.empty()) {
        filenum = static_cast<uint32_t>(nums[0]);
        has_filenum = true;
        if (parsed.has_binlog_offset && parsed.filenum == 0) {
          parsed.filenum = filenum;
        }
      }
    } else if (key == "master_lag") {
      auto nums = ExtractNumbers(value);
      if (!nums.empty()) {
        parsed.master_lag = static_cast<int64_t>(nums[0]);
        parsed.has_master_lag = true;
      }
    }
  }
  if (parsed.has_binlog_offset || parsed.has_master_lag) {
    *out = parsed;
    return true;
  }
  if (err) {
    *err = "replication info missing binlog_offset/master_lag";
  }
  return false;
}

bool FetchReplicationInfo(const std::string& host,
                          int port,
                          const std::string& passwd,
                          ReplicationInfo* out,
                          std::string* err) {
  if (!out) {
    return false;
  }
  std::unique_ptr<net::NetCli> cli(net::NewRedisCli());
  if (!cli) {
    if (err) {
      *err = "redis cli unavailable";
    }
    return false;
  }
  cli->set_connect_timeout(1500);
  cli->set_send_timeout(1500);
  cli->set_recv_timeout(1500);
  pstd::Status s = cli->Connect(host, port, "");
  if (!s.ok()) {
    if (err) {
      *err = "connect failed: " + s.ToString();
    }
    return false;
  }
  if (!passwd.empty()) {
    std::string auth_reply;
    if (!SendCommand(cli.get(), {"auth", passwd}, &auth_reply)) {
      if (err) {
        *err = "auth failed";
      }
      cli->Close();
      return false;
    }
    std::string auth_lower = ToLower(auth_reply);
    if (auth_lower != "ok") {
      if (err) {
        *err = "auth rejected: " + auth_reply;
      }
      cli->Close();
      return false;
    }
  }

  std::string info;
  if (!SendCommand(cli.get(), {"info", "replication"}, &info)) {
    if (err) {
      *err = "info replication failed";
    }
    cli->Close();
    return false;
  }
  cli->Close();

  if (!ParseReplicationInfo(info, out, err)) {
    return false;
  }
  return true;
}
