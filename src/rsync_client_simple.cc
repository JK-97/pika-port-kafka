#include "rsync_client_simple.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <thread>

#include <glog/logging.h>

#include "net/include/net_cli.h"
#include "pstd/include/env.h"
#include "pika_define.h"
#include "rsync_service.pb.h"

namespace {

const auto kRsyncMetaRetryMaxElapsed = std::chrono::minutes(30);
const int kRsyncMetaRetryLogEvery = 5;
const int kRsyncMetaRetrySleepMs = 1000;
const int kRsyncMetaBackoffInitialMs = 1000;
const int kRsyncMetaBackoffMaxMs = 15000;

std::string JoinPath(const std::string& base, const std::string& path) {
  if (base.empty()) {
    return path;
  }
  if (base.back() == '/') {
    return base + path;
  }
  return base + "/" + path;
}

std::string Dirname(const std::string& path) {
  size_t pos = path.find_last_of('/');
  if (pos == std::string::npos) {
    return "";
  }
  return path.substr(0, pos);
}

}  // namespace

RsyncClientSimple::RsyncClientSimple(const std::string& master_ip,
                                     int master_port,
                                     const std::string& db_name,
                                     const std::string& dump_path,
                                     size_t chunk_bytes,
                                     int timeout_ms)
    : master_ip_(master_ip),
      master_port_(master_port),
      db_name_(db_name),
      dump_path_(dump_path),
      chunk_bytes_(chunk_bytes),
      timeout_ms_(timeout_ms) {}

bool RsyncClientSimple::Fetch() {
  std::vector<std::string> files;
  snapshot_uuid_.clear();

  if (!pstd::DeleteDirIfExist(dump_path_)) {
    LOG(WARNING) << "rsync2: failed to clean dump path " << dump_path_;
    return false;
  }
  if (pstd::CreatePath(dump_path_) != 0) {
    LOG(WARNING) << "rsync2: failed to create dump path " << dump_path_;
    return false;
  }

  if (!FetchMeta(&files)) {
    return false;
  }

  for (const auto& file : files) {
    if (!FetchFile(file)) {
      return false;
    }
  }
  return true;
}

bool RsyncClientSimple::FetchMeta(std::vector<std::string>* files) {
  const auto start_time = std::chrono::steady_clock::now();
  int attempt = 0;
  int meta_error_count = 0;
  int backoff_ms = kRsyncMetaBackoffInitialMs;
  while (true) {
    auto elapsed = std::chrono::steady_clock::now() - start_time;
    if (elapsed >= kRsyncMetaRetryMaxElapsed) {
      auto elapsed_sec = std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
      LOG(WARNING) << "rsync2: meta request timeout after " << elapsed_sec << "s, attempts=" << attempt;
      return false;
    }
    ++attempt;
    std::unique_ptr<net::NetCli> cli(net::NewPbCli());
    cli->set_connect_timeout(timeout_ms_);
    cli->set_send_timeout(timeout_ms_);
    cli->set_recv_timeout(timeout_ms_);
    pstd::Status s = cli->Connect(master_ip_, master_port_ + kPortShiftRsync2, "");
    if (!s.ok()) {
      LOG(WARNING) << "rsync2: connect failed " << s.ToString();
      std::this_thread::sleep_for(std::chrono::milliseconds(kRsyncMetaRetrySleepMs));
      continue;
    }

    RsyncService::RsyncRequest request;
    request.set_type(RsyncService::kRsyncMeta);
    request.set_reader_index(0);
    request.set_db_name(db_name_);
    request.set_slot_id(0);

    s = cli->Send(&request);
    if (!s.ok()) {
      LOG(WARNING) << "rsync2: send meta request failed " << s.ToString();
      cli->Close();
      std::this_thread::sleep_for(std::chrono::milliseconds(kRsyncMetaRetrySleepMs));
      continue;
    }

    RsyncService::RsyncResponse response;
    s = cli->Recv(&response);
    cli->Close();
    if (!s.ok()) {
      LOG(WARNING) << "rsync2: recv meta response failed " << s.ToString();
      std::this_thread::sleep_for(std::chrono::milliseconds(kRsyncMetaRetrySleepMs));
      continue;
    }
    if (response.code() != RsyncService::kOk || !response.has_meta_resp()) {
      ++meta_error_count;
      elapsed = std::chrono::steady_clock::now() - start_time;
      auto elapsed_sec = std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
      if (meta_error_count == 1 || meta_error_count % kRsyncMetaRetryLogEvery == 0) {
        LOG(INFO) << "rsync2: meta not ready, waiting bgsave"
                  << " attempt=" << meta_error_count
                  << " elapsed=" << elapsed_sec << "s"
                  << " next_sleep=" << backoff_ms << "ms";
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
      backoff_ms = std::min(backoff_ms * 2, kRsyncMetaBackoffMaxMs);
      continue;
    }
    snapshot_uuid_ = response.snapshot_uuid();
    files->assign(response.meta_resp().filenames().begin(), response.meta_resp().filenames().end());
    return true;
  }
  return false;
}

bool RsyncClientSimple::EnsureParentDir(const std::string& file_path) {
  std::string dir = Dirname(file_path);
  if (dir.empty()) {
    return true;
  }
  int dir_state = pstd::IsDir(dir);
  if (dir_state == 0) {
    return true;
  }
  if (dir_state == 1) {
    LOG(WARNING) << "rsync2: path exists but is file " << dir;
    return false;
  }
  if (pstd::CreatePath(dir) != 0) {
    LOG(WARNING) << "rsync2: failed to create path " << dir;
    return false;
  }
  return true;
}

bool RsyncClientSimple::FetchFile(const std::string& filename) {
  std::unique_ptr<net::NetCli> cli(net::NewPbCli());
  cli->set_connect_timeout(timeout_ms_);
  cli->set_send_timeout(timeout_ms_);
  cli->set_recv_timeout(timeout_ms_);
  pstd::Status s = cli->Connect(master_ip_, master_port_ + kPortShiftRsync2, "");
  if (!s.ok()) {
    LOG(WARNING) << "rsync2: connect failed " << s.ToString();
    return false;
  }

  std::string local_path = JoinPath(dump_path_, filename);
  if (!EnsureParentDir(local_path)) {
    cli->Close();
    return false;
  }

  std::ofstream out(local_path, std::ios::binary | std::ios::out | std::ios::trunc);
  if (!out.is_open()) {
    LOG(WARNING) << "rsync2: open file failed " << local_path;
    cli->Close();
    return false;
  }

  uint64_t offset = 0;
  while (true) {
    RsyncService::RsyncRequest request;
    request.set_type(RsyncService::kRsyncFile);
    request.set_reader_index(0);
    request.set_db_name(db_name_);
    request.set_slot_id(0);
    RsyncService::FileRequest* file_req = request.mutable_file_req();
    file_req->set_filename(filename);
    file_req->set_offset(offset);
    file_req->set_count(chunk_bytes_);

    s = cli->Send(&request);
    if (!s.ok()) {
      LOG(WARNING) << "rsync2: send file request failed " << s.ToString();
      cli->Close();
      return false;
    }

    RsyncService::RsyncResponse response;
    s = cli->Recv(&response);
    if (!s.ok()) {
      LOG(WARNING) << "rsync2: recv file response failed " << s.ToString();
      cli->Close();
      return false;
    }
    if (response.code() != RsyncService::kOk || !response.has_file_resp()) {
      LOG(WARNING) << "rsync2: file response error";
      cli->Close();
      return false;
    }
    if (response.snapshot_uuid() != snapshot_uuid_) {
      LOG(WARNING) << "rsync2: snapshot uuid changed, expected " << snapshot_uuid_
                   << " actual " << response.snapshot_uuid();
      cli->Close();
      return false;
    }

    const auto& file_resp = response.file_resp();
    if (file_resp.offset() != offset) {
      LOG(WARNING) << "rsync2: unexpected offset " << file_resp.offset()
                   << " expected " << offset;
    }
    out.seekp(static_cast<std::streamoff>(offset), std::ios::beg);
    out.write(file_resp.data().data(), static_cast<std::streamsize>(file_resp.data().size()));
    if (!out.good()) {
      LOG(WARNING) << "rsync2: write failed " << local_path;
      cli->Close();
      return false;
    }

    offset += file_resp.count();
    if (file_resp.eof() != 0) {
      break;
    }
  }
  cli->Close();
  return true;
}
