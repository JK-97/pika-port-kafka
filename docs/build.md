# 编译说明

本项目依赖 PikiwiDB 源码构建出的 `pstd` / `net` / `storage` 静态库及其 `deps` 目录。
以下步骤在 PikiwiDB 3.5.6（sha: 510ae73081190fbd85ec37b40c067b2b1aa9e615）上验证通过。

## 1. 前置依赖

- GCC/G++ >= 9（C++17）
- CMake >= 3.18
- make
- librdkafka（开发包）
- 可选：libunwind / gperftools（若 PikiwiDB 构建默认启用）

示例（Ubuntu）：

```bash
apt-get update
apt-get install -y build-essential cmake librdkafka-dev libunwind-dev libgoogle-perftools-dev
```

示例（CentOS/Rocky）：

```bash
yum install -y gcc gcc-c++ make cmake librdkafka-devel libunwind-devel gperftools-devel
```

## 2. 编译 PikiwiDB 基础库

```bash
git clone https://github.com/OpenAtomFoundation/pikiwidb.git
cd pikiwidb
git checkout v3.5.6
```

构建：

```bash
cmake -S . -B build -DUSE_PIKA_TOOLS=OFF -DWITH_COMMAND_DOCS=OFF
cmake --build build --target pstd net storage -j$(nproc)
```

如果构建时缺 libunwind/gperftools，可安装依赖或在支持的 PikiwiDB 版本上使用：

```bash
cmake -S . -B build -DUSE_PIKA_TOOLS=OFF -DWITH_COMMAND_DOCS=OFF \
  -DENABLE_LIBUNWIND=OFF -DENABLE_GPERFTOOLS=OFF
```

确认生成：

- `build/src/pstd/libpstd.a`
- `build/src/net/libnet.a`
- `build/src/storage/libstorage.a`
- `deps/include` 与 `deps/lib` 目录存在

备注：如果 `deps/include` 缺少 `rediscache/commondef.h`，建议执行 PikiwiDB 的 `./build.sh`
以生成完整依赖，或单独构建 `rediscache` 目标。

## 3. 编译 pika-port-kafka

一键构建（PikiwiDB 基础库 + 本项目）：

```bash
./scripts/build_all.sh \
  --pikiwidb-root /path/to/pikiwidb \
  --pikiwidb-build /path/to/pikiwidb/build \
  --pikiwidb-deps /path/to/pikiwidb/deps
```

若本机已有 `/tmp/pikiwidb`，可直接运行：

```bash
./scripts/build_all.sh
```

如需自动拉取源码：

```bash
./scripts/build_all.sh --clone --pikiwidb-ref v3.5.6
```

推荐使用脚本（仅构建本项目）：

```bash
./scripts/build.sh \
  --pikiwidb-root /path/to/pikiwidb \
  --pikiwidb-build /path/to/pikiwidb/build \
  --pikiwidb-deps /path/to/pikiwidb/deps
```

如果 PikiwiDB 在 `/tmp/pikiwidb`，可直接运行：

```bash
./scripts/build.sh
```

查看脚本参数：

```bash
./scripts/build.sh --help
```

默认情况下脚本会禁用 `libunwind` 和 `gperftools`（可用 `--enable-libunwind` / `--enable-gperftools` 开启）。

手动方式（不使用脚本）：

```bash
cmake -S ./src -B ./build \
  -DPIKIWIDB_ROOT=/path/to/pikiwidb \
  -DPIKIWIDB_BUILD=/path/to/pikiwidb/build \
  -DPIKIWIDB_DEPS=/path/to/pikiwidb/deps \
  -DROCKSDB_LIBRARY=/path/to/pikiwidb/deps/lib/librocksdb.a \
  -DGLOG_LIBRARY=/path/to/pikiwidb/deps/lib/libglog.a \
  -DGFLAGS_LIBRARY=/path/to/pikiwidb/deps/lib/libgflags.a \
  -DSNAPPY_LIBRARY=/path/to/pikiwidb/deps/lib/libsnappy.a \
  -DZLIB_LIBRARY=/path/to/pikiwidb/deps/lib/libz.a \
  -DLZ4_LIBRARY=/path/to/pikiwidb/deps/lib/liblz4.a \
  -DZSTD_LIBRARY=/path/to/pikiwidb/deps/lib/libzstd.a \
  -DFMT_LIBRARY=/path/to/pikiwidb/deps/lib/libfmt.a \
  -DJEMALLOC_LIBRARY=/path/to/pikiwidb/deps/lib/libjemalloc.a \
  -DBZ2_LIBRARY=/path/to/system/libbz2.so
cmake --build ./build -j$(nproc)
```

`BZ2_LIBRARY` 路径可用以下方式确认：

```bash
ldconfig -p | rg libbz2
```

产物位置：`./build/pika_port`

## 4. 常见问题

- `cannot find -lrdkafka`：安装 `librdkafka` 开发包，或设置 `LIBRARY_PATH` 指向库目录。
- `undefined reference` to fmt/jemalloc/rocksdb：确认 CMake 参数中的 `*_LIBRARY` 指向真实文件。
