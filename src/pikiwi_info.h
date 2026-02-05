#ifndef PIKIWI_INFO_H_
#define PIKIWI_INFO_H_

#include <cstdint>
#include <string>

struct ReplicationInfo {
  bool has_binlog_offset{false};
  uint32_t filenum{0};
  uint64_t offset{0};
  bool has_master_lag{false};
  int64_t master_lag{0};
};

bool FetchReplicationInfo(const std::string& host,
                          int port,
                          const std::string& passwd,
                          ReplicationInfo* out,
                          std::string* err);

bool ParseReplicationInfo(const std::string& info, ReplicationInfo* out, std::string* err);

#endif  // PIKIWI_INFO_H_
