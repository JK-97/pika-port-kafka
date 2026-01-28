#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1090
source "${SCRIPT_DIR}/kafka_env.sh"

load_env
require_vars KAFKA_BROKER SNAPSHOT_TOPIC BINLOG_TOPIC OFFSETS_TOPIC \
  PARTITIONS_SNAPSHOT PARTITIONS_BINLOG PARTITIONS_OFFSETS REPLICATION_FACTOR
check_kafka_tooling

create_dual_topics

kafka_topics --describe --topic "$SNAPSHOT_TOPIC"
kafka_topics --describe --topic "$BINLOG_TOPIC"
kafka_topics --describe --topic "$OFFSETS_TOPIC"
