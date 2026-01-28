#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1090
source "${SCRIPT_DIR}/kafka_env.sh"

load_env
require_vars KAFKA_BROKER SINGLE_TOPIC OFFSETS_TOPIC \
  PARTITIONS_SINGLE PARTITIONS_OFFSETS REPLICATION_FACTOR \
  CHECKPOINT_PATH LOG_DIR RSYNC_DIR
check_kafka_tooling

reset_local_paths
delete_single_topics
create_single_topics

kafka_topics --describe --topic "$SINGLE_TOPIC"
kafka_topics --describe --topic "$OFFSETS_TOPIC"
