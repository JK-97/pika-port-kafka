#ifndef RESP_PARSER_H_
#define RESP_PARSER_H_

#include <string>

#include "net/include/redis_cli.h"

enum RespParseStatus {
  kRespOk = 0,
  kRespError = 1,
};

RespParseStatus ParseRedisRESPArray(const std::string& content, net::RedisCmdArgsType* argv);

#endif  // RESP_PARSER_H_
