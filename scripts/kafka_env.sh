#!/usr/bin/env bash
set -euo pipefail

die() {
  echo "ERROR: $*" >&2
  exit 1
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${ENV_FILE:-${REPO_ROOT}/.env}"

load_env() {
  if [[ ! -f "$ENV_FILE" ]]; then
    die ".env not found: $ENV_FILE"
  fi
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
}

require_vars() {
  local missing=0
  local name=""
  for name in "$@"; do
    if [[ -z "${!name:-}" ]]; then
      echo "Missing env: $name" >&2
      missing=1
    fi
  done
  if [[ "$missing" -ne 0 ]]; then
    exit 1
  fi
}

check_kafka_tooling() {
  if [[ -n "${KAFKA_CONTAINER:-}" ]]; then
    if ! command -v docker >/dev/null 2>&1; then
      die "docker not found but KAFKA_CONTAINER is set"
    fi
  else
    if ! command -v kafka-topics.sh >/dev/null 2>&1; then
      die "kafka-topics.sh not found and KAFKA_CONTAINER is empty"
    fi
  fi
}

kafka_topics() {
  if [[ -n "${KAFKA_CONTAINER:-}" ]]; then
    docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "$KAFKA_BROKER" "$@"
  else
    kafka-topics.sh --bootstrap-server "$KAFKA_BROKER" "$@"
  fi
}

kafka_configs() {
  if [[ -n "${KAFKA_CONTAINER:-}" ]]; then
    docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-configs.sh \
      --bootstrap-server "$KAFKA_BROKER" "$@"
  else
    kafka-configs.sh --bootstrap-server "$KAFKA_BROKER" "$@"
  fi
}

topic_exists() {
  kafka_topics --describe --topic "$1" >/dev/null 2>&1
}

delete_topic() {
  kafka_topics --delete --topic "$1" --if-exists || true
}

wait_topic_deleted() {
  local topic="$1"
  local i
  for i in $(seq 1 30); do
    if topic_exists "$topic"; then
      sleep 1
      continue
    fi
    return 0
  done
  echo "WARNING: topic still exists after 30s: $topic" >&2
}

create_topic() {
  local topic="$1"
  local partitions="$2"
  shift 2
  kafka_topics --create --topic "$topic" \
    --partitions "$partitions" --replication-factor "$REPLICATION_FACTOR" "$@"
}

safe_rm_dir() {
  local dir="$1"
  if [[ -z "$dir" || "$dir" == "/" ]]; then
    die "Refusing to remove dir: '$dir'"
  fi
  rm -rf "$dir"
  mkdir -p "$dir"
}

safe_rm_file() {
  local path="$1"
  if [[ -z "$path" || "$path" == "/" ]]; then
    die "Refusing to remove file: '$path'"
  fi
  rm -f "$path"
}

reset_local_paths() {
  if [[ -n "${PIKA_PORT_MATCH:-}" ]]; then
    if pgrep -f "$PIKA_PORT_MATCH" >/dev/null 2>&1; then
      echo "WARNING: pika_port appears to be running, stop it before reset." >&2
    fi
  fi
  safe_rm_file "$CHECKPOINT_PATH"
  safe_rm_dir "$RSYNC_DIR"
  safe_rm_dir "$LOG_DIR"
}

create_offsets_topic() {
  create_topic "$OFFSETS_TOPIC" "$PARTITIONS_OFFSETS"
  kafka_configs --alter --entity-type topics --entity-name "$OFFSETS_TOPIC" \
    --add-config cleanup.policy=compact
}

create_dual_topics() {
  create_topic "$SNAPSHOT_TOPIC" "$PARTITIONS_SNAPSHOT"
  create_topic "$BINLOG_TOPIC" "$PARTITIONS_BINLOG"
  create_offsets_topic
}

delete_dual_topics() {
  delete_topic "$SNAPSHOT_TOPIC"
  delete_topic "$BINLOG_TOPIC"
  delete_topic "$OFFSETS_TOPIC"
  wait_topic_deleted "$SNAPSHOT_TOPIC"
  wait_topic_deleted "$BINLOG_TOPIC"
  wait_topic_deleted "$OFFSETS_TOPIC"
}

create_single_topics() {
  create_topic "$SINGLE_TOPIC" "$PARTITIONS_SINGLE"
  create_offsets_topic
}

delete_single_topics() {
  delete_topic "$SINGLE_TOPIC"
  delete_topic "$OFFSETS_TOPIC"
  wait_topic_deleted "$SINGLE_TOPIC"
  wait_topic_deleted "$OFFSETS_TOPIC"
}
