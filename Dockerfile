FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    libstdc++6 \
    librdkafka1 \
    libgflags2.2 \
    libgoogle-glog0v5 \
    libsnappy1v5 \
    zlib1g \
    libbz2-1.0 \
    liblz4-1 \
    libzstd1 \
    libjemalloc2 \
    libunwind8 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /data/devops/pika_ops

COPY ./pika_port /data/devops/pika_ops/pika_port
RUN chmod +x /data/devops/pika_ops/pika_port

ENTRYPOINT ["/data/devops/pika_ops/pika_port"]
