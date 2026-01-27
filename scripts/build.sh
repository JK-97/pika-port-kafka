#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/build.sh [options]

Options:
  --pikiwidb-root PATH    PikiwiDB source root
  --pikiwidb-build PATH   PikiwiDB build dir (contains build/src/...)
  --pikiwidb-deps PATH    PikiwiDB deps dir (contains deps/include deps/lib)
  --unwind-lib PATH       libunwind.so path (optional)
  --unwind-x86-64-lib PATH libunwind-x86_64.so path (optional)
  --protobuf-lib PATH     libprotobuf.a path (optional)
  --protobuf-protoc PATH  protoc path (optional)
  --bz2-lib PATH          libbz2.so path (optional)
  --build-dir PATH        This repo build dir (default: ./build)
  --src-dir PATH          This repo source dir (default: ./src)
  --jobs N                Parallel build jobs (default: nproc)
  --clean                 Remove build dir before build
  -h, --help              Show help
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

detect_bz2() {
  local path=""
  if command -v ldconfig >/dev/null 2>&1; then
    path="$(ldconfig -p | awk '/libbz2\.so/ {print $NF; exit}')"
  fi
  if [[ -z "$path" ]]; then
    for p in /lib*/libbz2.so* /usr/lib*/libbz2.so*; do
      if [[ -e "$p" ]]; then
        path="$p"
        break
      fi
    done
  fi
  echo "$path"
}

detect_unwind() {
  local name="$1"
  local path=""
  if command -v ldconfig >/dev/null 2>&1; then
    path="$(ldconfig -p | awk -v n="$name" '$1 == n {print $NF; exit}')"
  fi
  if [[ -z "$path" ]]; then
    for p in /lib*/"$name" /usr/lib*/"$name"; do
      if [[ -e "$p" ]]; then
        path="$p"
        break
      fi
    done
  fi
  echo "$path"
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
BZ2_LIBRARY="${BZ2_LIBRARY:-}"
BUILD_DIR="${REPO_ROOT}/build"
SRC_DIR="${REPO_ROOT}/src"
JOBS=""
CLEAN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pikiwidb-root) PIKIWIDB_ROOT="$2"; shift 2 ;;
    --pikiwidb-build) PIKIWIDB_BUILD="$2"; shift 2 ;;
    --pikiwidb-deps) PIKIWIDB_DEPS="$2"; shift 2 ;;
    --unwind-lib) UNWIND_LIBRARY="$2"; shift 2 ;;
    --unwind-x86-64-lib) UNWIND_X86_64_LIBRARY="$2"; shift 2 ;;
    --protobuf-lib) PROTOBUF_LIBRARY="$2"; shift 2 ;;
    --protobuf-protoc) PROTOBUF_PROTOC="$2"; shift 2 ;;
    --bz2-lib) BZ2_LIBRARY="$2"; shift 2 ;;
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
  die "Missing --pikiwidb-root or PIKIWIDB_ROOT"
fi
[[ -d "$PIKIWIDB_ROOT" ]] || die "PIKIWIDB_ROOT not found: $PIKIWIDB_ROOT"

if [[ -z "$PIKIWIDB_BUILD" && -d "${PIKIWIDB_ROOT}/build" ]]; then
  PIKIWIDB_BUILD="${PIKIWIDB_ROOT}/build"
fi
[[ -n "$PIKIWIDB_BUILD" ]] || die "Missing --pikiwidb-build or PIKIWIDB_BUILD"
[[ -d "$PIKIWIDB_BUILD" ]] || die "PIKIWIDB_BUILD not found: $PIKIWIDB_BUILD"

if [[ -z "$PIKIWIDB_DEPS" && -d "${PIKIWIDB_ROOT}/deps" ]]; then
  PIKIWIDB_DEPS="${PIKIWIDB_ROOT}/deps"
fi
[[ -n "$PIKIWIDB_DEPS" ]] || die "Missing --pikiwidb-deps or PIKIWIDB_DEPS"
[[ -d "$PIKIWIDB_DEPS" ]] || die "PIKIWIDB_DEPS not found: $PIKIWIDB_DEPS"

if [[ -z "$BZ2_LIBRARY" ]]; then
  BZ2_LIBRARY="$(detect_bz2)"
fi
[[ -n "$BZ2_LIBRARY" ]] || die "libbz2.so not found; use --bz2-lib to set a path"

if [[ -z "$UNWIND_LIBRARY" ]]; then
  UNWIND_LIBRARY="$(detect_unwind libunwind.so)"
fi
if [[ -z "$UNWIND_X86_64_LIBRARY" ]]; then
  UNWIND_X86_64_LIBRARY="$(detect_unwind libunwind-x86_64.so)"
fi

if [[ -z "$JOBS" ]]; then
  if command -v nproc >/dev/null 2>&1; then
    JOBS="$(nproc)"
  else
    JOBS="4"
  fi
fi

[[ -d "$SRC_DIR" ]] || die "Source dir not found: $SRC_DIR"

DEPS_LIB="${PIKIWIDB_DEPS}/lib"
[[ -d "$DEPS_LIB" ]] || die "Deps dir not found: $DEPS_LIB"

require_lib() {
  local path="$1"
  [[ -f "$path" ]] || die "Missing library: $path"
}

require_lib "${DEPS_LIB}/librocksdb.a"
require_lib "${DEPS_LIB}/libglog.a"
require_lib "${DEPS_LIB}/libgflags.a"
require_lib "${DEPS_LIB}/libsnappy.a"
require_lib "${DEPS_LIB}/libz.a"
require_lib "${DEPS_LIB}/liblz4.a"
require_lib "${DEPS_LIB}/libzstd.a"
require_lib "${DEPS_LIB}/libfmt.a"
require_lib "${DEPS_LIB}/libjemalloc.a"
if [[ -z "$PROTOBUF_LIBRARY" && -f "${DEPS_LIB}/libprotobuf.a" ]]; then
  PROTOBUF_LIBRARY="${DEPS_LIB}/libprotobuf.a"
fi
if [[ -z "$PROTOBUF_PROTOC" && -x "${PIKIWIDB_DEPS}/bin/protoc" ]]; then
  PROTOBUF_PROTOC="${PIKIWIDB_DEPS}/bin/protoc"
fi

if [[ "$CLEAN" -eq 1 ]]; then
  rm -rf "$BUILD_DIR"
fi

cmake -S "$SRC_DIR" -B "$BUILD_DIR" \
  -DPIKIWIDB_ROOT="$PIKIWIDB_ROOT" \
  -DPIKIWIDB_BUILD="$PIKIWIDB_BUILD" \
  -DPIKIWIDB_DEPS="$PIKIWIDB_DEPS" \
  -DROCKSDB_LIBRARY="${DEPS_LIB}/librocksdb.a" \
  -DGLOG_LIBRARY="${DEPS_LIB}/libglog.a" \
  -DGFLAGS_LIBRARY="${DEPS_LIB}/libgflags.a" \
  -DSNAPPY_LIBRARY="${DEPS_LIB}/libsnappy.a" \
  -DZLIB_LIBRARY="${DEPS_LIB}/libz.a" \
  -DLZ4_LIBRARY="${DEPS_LIB}/liblz4.a" \
  -DZSTD_LIBRARY="${DEPS_LIB}/libzstd.a" \
  -DFMT_LIBRARY="${DEPS_LIB}/libfmt.a" \
  -DJEMALLOC_LIBRARY="${DEPS_LIB}/libjemalloc.a" \
  -DBZ2_LIBRARY="$BZ2_LIBRARY" \
  ${UNWIND_LIBRARY:+-DUNWIND_LIBRARY="$UNWIND_LIBRARY"} \
  ${UNWIND_X86_64_LIBRARY:+-DUNWIND_X86_64_LIBRARY="$UNWIND_X86_64_LIBRARY"} \
  ${PROTOBUF_LIBRARY:+-DPROTOBUF_LIBRARY="$PROTOBUF_LIBRARY"} \
  ${PROTOBUF_PROTOC:+-DPROTOBUF_PROTOC="$PROTOBUF_PROTOC"}

cmake --build "$BUILD_DIR" -j"$JOBS"
