#ifndef EVENT_BUILDER_H_
#define EVENT_BUILDER_H_

#include <string>

#include "binlog_transverter.h"
#include "net/include/redis_cli.h"

std::string BuildPartitionKey(const std::string& db_name, const std::string& data_type, const std::string& key);
std::string CommandDataType(const std::string& cmd);

std::string BuildSnapshotEventJson(const net::RedisCmdArgsType& argv,
                                   const std::string& db_name,
                                   const std::string& data_type,
                                   const std::string& source_id,
                                   const std::string& raw_resp,
                                   const std::string& key);

std::string BuildBinlogEventJson(const net::RedisCmdArgsType& argv,
                                 const PortBinlogItem& item,
                                 const std::string& db_name,
                                 const std::string& data_type,
                                 const std::string& source_id,
                                 const std::string& raw_resp,
                                 const std::string& key);

#endif  // EVENT_BUILDER_H_
