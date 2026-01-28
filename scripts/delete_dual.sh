#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1090
source "${SCRIPT_DIR}/kafka_env.sh"

load_env
require_vars KAFKA_BROKER SNAPSHOT_TOPIC BINLOG_TOPIC OFFSETS_TOPIC
check_kafka_tooling

delete_dual_topics
