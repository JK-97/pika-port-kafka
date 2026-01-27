#ifndef RSYNC_CLIENT_SIMPLE_H_
#define RSYNC_CLIENT_SIMPLE_H_

#include <cstddef>
#include <string>
#include <vector>

class RsyncClientSimple {
 public:
  RsyncClientSimple(const std::string& master_ip,
                    int master_port,
                    const std::string& db_name,
                    const std::string& dump_path,
                    size_t chunk_bytes,
                    int timeout_ms);

  bool Fetch();
  const std::string& snapshot_uuid() const { return snapshot_uuid_; }

 private:
  bool FetchMeta(std::vector<std::string>* files);
  bool FetchFile(const std::string& filename);
  bool EnsureParentDir(const std::string& file_path);

 private:
  std::string master_ip_;
  int master_port_;
  std::string db_name_;
  std::string dump_path_;
  size_t chunk_bytes_;
  int timeout_ms_;
  std::string snapshot_uuid_;
};

#endif  // RSYNC_CLIENT_SIMPLE_H_
