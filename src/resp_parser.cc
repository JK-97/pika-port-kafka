#include "resp_parser.h"

#include <cassert>

#include <glog/logging.h>

#include "pstd/include/pstd_string.h"

namespace {

int32_t FindNextSeparators(const std::string& content, int32_t pos) {
  int32_t length = static_cast<int32_t>(content.size());
  if (pos >= length) {
    return -1;
  }
  while (pos < length) {
    if (content[pos] == '\n') {
      return pos;
    }
    pos++;
  }
  return -1;
}

int32_t GetNextNum(const std::string& content, int32_t left_pos, int32_t right_pos, long* value) {
  assert(left_pos < right_pos);
  if (pstd::string2int(content.data() + left_pos + 1, right_pos - left_pos - 2, value) != 0) {
    return 0;
  }
  return -1;
}

}  // namespace

// RedisRESPArray : *3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
RespParseStatus ParseRedisRESPArray(const std::string& content, net::RedisCmdArgsType* argv) {
  int32_t pos = 0;
  int32_t next_parse_pos = 0;
  int32_t content_len = static_cast<int32_t>(content.size());
  long multibulk_len = 0;
  long bulk_len = 0;
  if (content.empty() || content[0] != '*') {
    LOG(INFO) << "Content empty() or the first character of the redis protocol string not equal '*'";
    return kRespError;
  }
  pos = FindNextSeparators(content, next_parse_pos);
  if (pos != -1 && GetNextNum(content, next_parse_pos, pos, &multibulk_len) != -1) {
    next_parse_pos = pos + 1;
  } else {
    LOG(INFO) << "Find next separators error or get next num error";
    return kRespError;
  }

  argv->clear();
  while (multibulk_len != 0) {
    if (content[next_parse_pos] != '$') {
      LOG(INFO) << "The first charactor of the RESP type element not equal '$'";
      return kRespError;
    }

    bulk_len = -1;
    pos = FindNextSeparators(content, next_parse_pos);
    if (pos != -1 && GetNextNum(content, next_parse_pos, pos, &bulk_len) != -1) {
      if (pos + 1 + bulk_len + 2 > content_len) {
        return kRespError;
      } else {
        next_parse_pos = pos + 1;
        argv->emplace_back(content.data() + next_parse_pos, bulk_len);
        next_parse_pos = next_parse_pos + bulk_len + 2;
        multibulk_len--;
      }
    } else {
      LOG(INFO) << "Find next separators error or get next num error";
      return kRespError;
    }
  }
  if (content_len != next_parse_pos) {
    LOG(INFO) << "Incomplete parse";
    return kRespError;
  }
  return kRespOk;
}
