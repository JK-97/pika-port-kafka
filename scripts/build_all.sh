#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/build_all.sh [options]

Builds PikiwiDB base libs (pstd/net/storage) and then builds pika-port-kafka.

Options:
  --pikiwidb-root PATH    PikiwiDB source root
  --pikiwidb-build PATH   PikiwiDB build dir (default: <root>/build)
  --pikiwidb-deps PATH    PikiwiDB deps dir (default: <root>/deps)
  --unwind-lib PATH       libunwind.so path (optional)
  --unwind-x86-64-lib PATH libunwind-x86_64.so path (optional)
  --protobuf-lib PATH     libprotobuf.a path (optional)
  --protobuf-protoc PATH  protoc path (optional)
  --pikiwidb-repo URL     PikiwiDB repo URL (default: official repo)
  --pikiwidb-ref REF      PikiwiDB tag/branch/commit (default: v3.5.6)
  --clone                 Clone if root does not exist
  --enable-libunwind      Pass ENABLE_LIBUNWIND=ON (default: OFF)
  --enable-gperftools     Pass ENABLE_GPERFTOOLS=ON (default: OFF)
  --build-dir PATH        This repo build dir (default: ./build)
  --src-dir PATH          This repo source dir (default: ./src)
  --jobs N                Parallel build jobs (default: nproc)
  --clean                 Remove build dirs before build
  -h, --help              Show help
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

patch_pikiwidb_cmake() {
  local cmake_file="$1"
  [[ -f "$cmake_file" ]] || return 0

  local py="python3"
  if ! command -v "$py" >/dev/null 2>&1; then
    py="python"
  fi

  "$py" - "$cmake_file" <<'PY'
import os
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text()

if "ENABLE_LIBUNWIND" in text or "ENABLE_GPERFTOOLS" in text:
    sys.exit(0)

changed = False

def replace_once(old, new, label):
    global text, changed
    if old not in text:
        print(f"WARNING: {label} not found in {path}", file=sys.stderr)
        return False
    text = text.replace(old, new, 1)
    changed = True
    return True

project_match = re.search(r"^project\\(.*\\)\\s*$", text, re.M)
if not project_match:
    print(f"WARNING: project() line not found in {path}", file=sys.stderr)
else:
    insert = (
        "option(ENABLE_LIBUNWIND \"Enable libunwind (for glog/gperftools)\" ON)\n"
        "option(ENABLE_GPERFTOOLS \"Enable gperftools\" ON)\n"
    )
    idx = project_match.end()
    text = text[:idx] + "\n" + insert + text[idx:]
    changed = True

replace_once(
    "if(CMAKE_SYSTEM_NAME MATCHES \"Linux\")\n  ExternalProject_Add(unwind",
    "if(CMAKE_SYSTEM_NAME MATCHES \"Linux\" AND ENABLE_LIBUNWIND)\n  ExternalProject_Add(unwind",
    "unwind ExternalProject_Add guard",
)
replace_once(
    "else()\n  set(LIBUNWIND_ON OFF)\nendif()",
    "else()\n  set(LIBUNWIND_ON OFF)\n  set(LIBUNWIND_NAME \"\")\nendif()",
    "LIBUNWIND_ON else block",
)
replace_once(
    "if(CMAKE_SYSTEM_NAME MATCHES \"Linux\")\n  ExternalProject_Add(gperftools",
    "if(CMAKE_SYSTEM_NAME MATCHES \"Linux\" AND ENABLE_GPERFTOOLS)\n  ExternalProject_Add(gperftools",
    "gperftools ExternalProject_Add guard",
)
replace_once(
    "  set(LIBGPERF_NAME gperftools)\nendif()",
    "  set(LIBGPERF_NAME gperftools)\nelse()\n  set(LIBGPERF_NAME \"\")\nendif()",
    "LIBGPERF_NAME else block",
)

if changed:
    path.write_text(text)
PY
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PIKIWIDB_ROOT="${PIKIWIDB_ROOT:-}"
PIKIWIDB_BUILD="${PIKIWIDB_BUILD:-}"
PIKIWIDB_DEPS="${PIKIWIDB_DEPS:-}"
UNWIND_LIBRARY="${UNWIND_LIBRARY:-}"
UNWIND_X86_64_LIBRARY="${UNWIND_X86_64_LIBRARY:-}"
PROTOBUF_LIBRARY="${PROTOBUF_LIBRARY:-}"
PROTOBUF_PROTOC="${PROTOBUF_PROTOC:-}"
PIKIWIDB_REPO="https://github.com/OpenAtomFoundation/pikiwidb.git"
PIKIWIDB_REF="v3.5.6"
BUILD_DIR="${REPO_ROOT}/build"
SRC_DIR="${REPO_ROOT}/src"
JOBS=""
CLONE=0
CLEAN=0
ENABLE_LIBUNWIND=-1
ENABLE_GPERFTOOLS=-1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pikiwidb-root) PIKIWIDB_ROOT="$2"; shift 2 ;;
    --pikiwidb-build) PIKIWIDB_BUILD="$2"; shift 2 ;;
    --pikiwidb-deps) PIKIWIDB_DEPS="$2"; shift 2 ;;
    --unwind-lib) UNWIND_LIBRARY="$2"; shift 2 ;;
    --unwind-x86-64-lib) UNWIND_X86_64_LIBRARY="$2"; shift 2 ;;
    --protobuf-lib) PROTOBUF_LIBRARY="$2"; shift 2 ;;
    --protobuf-protoc) PROTOBUF_PROTOC="$2"; shift 2 ;;
    --pikiwidb-repo) PIKIWIDB_REPO="$2"; shift 2 ;;
    --pikiwidb-ref) PIKIWIDB_REF="$2"; shift 2 ;;
    --clone) CLONE=1; shift ;;
    --enable-libunwind) ENABLE_LIBUNWIND=1; shift ;;
    --enable-gperftools) ENABLE_GPERFTOOLS=1; shift ;;
    --build-dir) BUILD_DIR="$2"; shift 2 ;;
    --src-dir) SRC_DIR="$2"; shift 2 ;;
    --jobs) JOBS="$2"; shift 2 ;;
    --clean) CLEAN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

if [[ -z "$PIKIWIDB_ROOT" && -d /tmp/pikiwidb ]]; then
  PIKIWIDB_ROOT="/tmp/pikiwidb"
fi
if [[ -z "$PIKIWIDB_ROOT" ]]; then
  if [[ "$CLONE" -eq 1 ]]; then
    PIKIWIDB_ROOT="/tmp/pikiwidb"
  else
    die "Missing --pikiwidb-root; use --clone to auto-fetch"
  fi
fi

if [[ ! -d "$PIKIWIDB_ROOT" ]]; then
  if [[ "$CLONE" -eq 1 ]]; then
    git clone "$PIKIWIDB_REPO" "$PIKIWIDB_ROOT"
    (cd "$PIKIWIDB_ROOT" && git checkout "$PIKIWIDB_REF")
  else
    die "PIKIWIDB_ROOT not found: $PIKIWIDB_ROOT"
  fi
fi

if [[ -z "$PIKIWIDB_BUILD" ]]; then
  PIKIWIDB_BUILD="${PIKIWIDB_ROOT}/build"
fi
if [[ -z "$PIKIWIDB_DEPS" ]]; then
  PIKIWIDB_DEPS="${PIKIWIDB_ROOT}/deps"
fi

if [[ -z "$JOBS" ]]; then
  if command -v nproc >/dev/null 2>&1; then
    JOBS="$(nproc)"
  else
    JOBS="4"
  fi
fi

if [[ "$ENABLE_LIBUNWIND" -lt 0 || "$ENABLE_GPERFTOOLS" -lt 0 ]]; then
  HOST_ARCH="$(uname -m 2>/dev/null || echo unknown)"
  case "$HOST_ARCH" in
    aarch64|arm64)
      [[ "$ENABLE_LIBUNWIND" -lt 0 ]] && ENABLE_LIBUNWIND=0
      [[ "$ENABLE_GPERFTOOLS" -lt 0 ]] && ENABLE_GPERFTOOLS=0
      ;;
    *)
      [[ "$ENABLE_LIBUNWIND" -lt 0 ]] && ENABLE_LIBUNWIND=1
      [[ "$ENABLE_GPERFTOOLS" -lt 0 ]] && ENABLE_GPERFTOOLS=1
      ;;
  esac
fi

if [[ "$CLEAN" -eq 1 ]]; then
  rm -rf "$PIKIWIDB_BUILD" "$BUILD_DIR"
fi

patch_pikiwidb_cmake "${PIKIWIDB_ROOT}/CMakeLists.txt"

cmake_args=(
  -S "$PIKIWIDB_ROOT"
  -B "$PIKIWIDB_BUILD"
  -DUSE_PIKA_TOOLS=OFF
  -DWITH_COMMAND_DOCS=OFF
  -DCMAKE_BUILD_TYPE=Release
)
if [[ "$ENABLE_LIBUNWIND" -eq 0 ]]; then
  cmake_args+=(-DENABLE_LIBUNWIND=OFF)
fi
if [[ "$ENABLE_GPERFTOOLS" -eq 0 ]]; then
  cmake_args+=(-DENABLE_GPERFTOOLS=OFF)
fi

cmake "${cmake_args[@]}"
cmake --build "$PIKIWIDB_BUILD" --target pstd net storage -j"$JOBS"

"${REPO_ROOT}/scripts/build.sh" \
  --pikiwidb-root "$PIKIWIDB_ROOT" \
  --pikiwidb-build "$PIKIWIDB_BUILD" \
  --pikiwidb-deps "$PIKIWIDB_DEPS" \
  --build-dir "$BUILD_DIR" \
  --src-dir "$SRC_DIR" \
  --jobs "$JOBS" \
  ${UNWIND_LIBRARY:+--unwind-lib "$UNWIND_LIBRARY"} \
  ${UNWIND_X86_64_LIBRARY:+--unwind-x86-64-lib "$UNWIND_X86_64_LIBRARY"} \
  ${PROTOBUF_LIBRARY:+--protobuf-lib "$PROTOBUF_LIBRARY"} \
  ${PROTOBUF_PROTOC:+--protobuf-protoc "$PROTOBUF_PROTOC"}
