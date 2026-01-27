// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>

#include <utility>

#include "pstd/include/pstd_coding.h"
#include "pstd/include/pstd_string.h"

#include "binlog_receiver_thread.h"
#include "binlog_transverter.h"
#include "const.h"
#include "master_conn.h"
#include "pika_port.h"
#include "resp_parser.h"

extern PikaPort* g_pika_port;

MasterConn::MasterConn(int fd, std::string ip_port, void* worker_specific_data)
    : NetConn(fd, std::move(ip_port), nullptr),
      rbuf_(nullptr),
      rbuf_len_(0),
      rbuf_size_(REDIS_IOBUF_LEN),
      rbuf_cur_pos_(0),
      is_authed_(false) {
  binlog_receiver_ = reinterpret_cast<BinlogReceiverThread*>(worker_specific_data);
  rbuf_ = static_cast<char*>(realloc(rbuf_, REDIS_IOBUF_LEN));
}

MasterConn::~MasterConn() { free(rbuf_); }

net::ReadStatus MasterConn::ReadRaw(uint32_t count) {
  if (rbuf_cur_pos_ + count > rbuf_size_) {
    return net::kFullError;
  }
  int32_t nread = read(fd(), rbuf_ + rbuf_len_, count - (rbuf_len_ - rbuf_cur_pos_));
  if (nread == -1) {
    if (errno == EAGAIN) {
      return net::kReadHalf;
    } else {
      return net::kReadError;
    }
  } else if (nread == 0) {
    return net::kReadClose;
  }

  rbuf_len_ += nread;
  if (rbuf_len_ - rbuf_cur_pos_ != count) {
    return net::kReadHalf;
  }
  return net::kReadAll;
}

net::ReadStatus MasterConn::ReadHeader() {
  if (rbuf_len_ >= HEADER_LEN) {
    return net::kReadAll;
  }

  net::ReadStatus status = ReadRaw(HEADER_LEN);
  if (status != net::kReadAll) {
    return status;
  }
  rbuf_cur_pos_ += HEADER_LEN;
  return net::kReadAll;
}

net::ReadStatus MasterConn::ReadBody(uint32_t body_length) {
  if (rbuf_len_ == HEADER_LEN + body_length) {
    return net::kReadAll;
  } else if (rbuf_len_ > HEADER_LEN + body_length) {
    LOG(INFO) << "rbuf_len_ larger than sum of header length (6 Byte)"
              << " and body_length, rbuf_len_: " << rbuf_len_ << ", body_length: " << body_length;
  }

  net::ReadStatus status = ReadRaw(body_length);
  if (status != net::kReadAll) {
    return status;
  }
  rbuf_cur_pos_ += body_length;
  return net::kReadAll;
}

void MasterConn::ResetStatus() {
  rbuf_len_ = 0;
  rbuf_cur_pos_ = 0;
}

net::ReadStatus MasterConn::GetRequest() {
  // Read Header
  net::ReadStatus status;
  if ((status = ReadHeader()) != net::kReadAll) {
    return status;
  }

  // Process Header, get body length;
  uint16_t type = 0;
  uint32_t body_length = 0;
  std::string header(rbuf_, HEADER_LEN);
  pstd::GetFixed16(&header, &type);
  pstd::GetFixed32(&header, &body_length);

  if (type != kTypePortAuth && type != kTypePortBinlog) {
    LOG(INFO) << "Unrecognizable Type: " << type << " maybe identify binlog type error";
    return net::kParseError;
  }

  // Realloc buffer according header_len and body_length if need
  uint32_t needed_size = HEADER_LEN + body_length;
  if (rbuf_size_ < needed_size) {
    if (needed_size > REDIS_MAX_MESSAGE) {
      return net::kFullError;
    } else {
      rbuf_ = static_cast<char*>(realloc(rbuf_, needed_size));
      rbuf_size_ = needed_size;
    }
  }

  // Read Body
  if ((status = ReadBody(body_length)) != net::kReadAll) {
    return status;
  }

  net::RedisCmdArgsType argv;
  std::string body(rbuf_ + HEADER_LEN, body_length);
  if (type == kTypePortAuth) {
    if (ParseRedisRESPArray(body, &argv) != kRespOk) {
      LOG(INFO) << "Type auth ParseRedisRESPArray error";
      return net::kParseError;
    }
    if (!ProcessAuth(argv)) {
      return net::kDealError;
    }
  } else if (type == kTypePortBinlog) {
    PortBinlogItem item;
    if (!PortBinlogTransverter::PortBinlogDecode(PortTypeFirst, body, &item)) {
      LOG(INFO) << "Binlog decode error: " << item.ToString();
      return net::kParseError;
    }
    if (ParseRedisRESPArray(item.content(), &argv) != kRespOk) {
      LOG(INFO) << "Type Binlog ParseRedisRESPArray error: " << item.ToString();
      return net::kParseError;
    }
    if (!ProcessBinlogData(argv, item)) {
      return net::kDealError;
    }
  } else {
    LOG(INFO) << "Unrecognizable Type";
    return net::kParseError;
  }

  // Reset status
  ResetStatus();
  return net::kReadAll;
}

net::WriteStatus MasterConn::SendReply() { return net::kWriteAll; }

void MasterConn::TryResizeBuffer() {}

bool MasterConn::ProcessAuth(const net::RedisCmdArgsType& argv) {
  if (argv.empty() || argv.size() != 2) {
    return false;
  }

  if (argv[0] == "auth") {
    if (argv[1] == std::to_string(g_pika_port->sid())) {
      is_authed_ = true;
      LOG(INFO) << "BinlogReceiverThread AccessHandle succeeded, My server id: " << g_pika_port->sid()
                << ", Master auth server id: " << argv[1];
      return true;
    }
  }

  LOG(INFO) << "BinlogReceiverThread AccessHandle failed, My server id: " << g_pika_port->sid()
            << ", Master auth server id: " << argv[1];

  return false;
}

bool MasterConn::ProcessBinlogData(const net::RedisCmdArgsType& argv, const PortBinlogItem& binlog_item) {
  if (!is_authed_) {
    LOG(INFO) << "Need Auth First";
    return false;
  } else if (argv.empty()) {
    return false;
  }

  std::string key;
  if (argv.size() > 1) {
    key = argv[1];
  }

  std::string command;
  if (argv[0] == "pksetexat"){
    //struct timeval now;
    std::string temp("");
    std::string time_out("");
    std::string time_cmd("");
    int start;
    int old_time_size;
    int new_time_size;
    int diff;
    temp = argv[2];
    //gettimeofday(&now, NULL);
    unsigned long int sec= time(NULL);
    unsigned long int tot;
    tot = std::stol(temp) - sec;
    time_out = std::to_string(tot);
    
    command = binlog_item.content();
    command.erase(0,4);
    command.replace(0, 13, "*4\r\n$5\r\nsetex");
    //"*4\r\n$5\r\nsetex\r\n$48\r\n1691478611637921200018685540810_4932190141418052\r\n$10\r\n1691483848\r\n$1681\r\n(\265/\375`\332\024=4"}}
    start = 13 + 3 + std::to_string(key.size()).size() + 2 + key.size() +3;
    old_time_size = std::to_string(temp.size()).size() + 2 + temp.size();
    new_time_size = std::to_string(time_out.size()).size() + 2 + time_out.size();
    diff =  old_time_size - new_time_size;
    command.erase(start, diff);
    time_cmd = std::to_string(time_out.size()) + "\r\n" + time_out;
    command.replace(start, new_time_size, time_cmd);
  } else {
    command = binlog_item.content();
  }
  
  int ret = g_pika_port->PublishBinlogEvent(argv, binlog_item, command, key);
  if (ret != 0) {
    LOG(WARNING) << "publish binlog event failed, ret:" << ret;
  }

  return true;
}
