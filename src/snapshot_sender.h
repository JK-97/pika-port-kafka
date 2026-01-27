#ifndef SNAPSHOT_SENDER_H_
#define SNAPSHOT_SENDER_H_

#include <cstdint>

#include "conf.h"
#include "checkpoint.h"

class SnapshotSender {
 public:
  SnapshotSender(const Conf& conf, CheckpointManager* checkpoint_manager);
  int Run();

 private:
  const Conf& conf_;
  CheckpointManager* checkpoint_manager_;
};

#endif  // SNAPSHOT_SENDER_H_
