#include "kafka_sender.h"

#include <chrono>
#include <utility>

#include <glog/logging.h>

#include "json_utils.h"

namespace {

std::string TrimForLog(const std::string& value, size_t max_len) {
  if (value.size() <= max_len) {
    return value;
  }
  return value.substr(0, max_len) + "...(" + std::to_string(value.size()) + ")";
}

std::string FormatKeyForLog(const std::string& key) {
  if (key.empty()) {
    return "";
  }
  if (IsPrintableAscii(key)) {
    return TrimForLog(key, 256);
  }
  return TrimForLog("b64:" + Base64Encode(key), 512);
}

}  // namespace

KafkaSender::KafkaSender(int id, const Conf& conf, CheckpointManager* checkpoint_manager)
    : id_(id), conf_(conf), checkpoint_manager_(checkpoint_manager), producer_(nullptr), should_exit_(false), elements_(0) {}

KafkaSender::~KafkaSender() {
  Stop();
  LOG(INFO) << "KafkaSender thread " << id_ << " exit!!!";
}

KafkaSender::StatsSnapshot KafkaSender::GetStatsSnapshot() const {
  StatsSnapshot snapshot;
  snapshot.queue_size = queue_size_.load(std::memory_order_relaxed);
  snapshot.outq_len = outq_len_.load(std::memory_order_relaxed);
  snapshot.send_total = send_total_.load(std::memory_order_relaxed);
  snapshot.ack_total = ack_total_.load(std::memory_order_relaxed);
  snapshot.ack_err_total = ack_err_total_.load(std::memory_order_relaxed);
  snapshot.produce_err_total = produce_err_total_.load(std::memory_order_relaxed);
  return snapshot;
}

void KafkaSender::Enqueue(const KafkaRecord& record) {
  std::unique_lock lock(queue_mutex_);
  if (queue_.size() < 100000) {
    queue_.push(record);
    queue_size_.fetch_add(1, std::memory_order_relaxed);
    queue_signal_.notify_one();
    return;
  }

  LOG(WARNING) << "KafkaSender queue full, waiting to enqueue (id=" << id_
               << " size=" << queue_.size() << ")";

  queue_signal_.wait(lock, [this] { return queue_.size() <= 100000 || should_exit_.load(); });
  if (should_exit_) {
    return;
  }
  queue_.push(record);
  queue_size_.fetch_add(1, std::memory_order_relaxed);
  queue_signal_.notify_one();
}

void KafkaSender::Stop() {
  should_exit_ = true;
  queue_signal_.notify_one();
}

bool KafkaSender::InitProducer() {
  char errstr[512];
  rd_kafka_conf_t* conf = rd_kafka_conf_new();
  rd_kafka_conf_set(conf, "bootstrap.servers", conf_.kafka_brokers.c_str(), errstr, sizeof(errstr));
  rd_kafka_conf_set(conf, "client.id", conf_.kafka_client_id.c_str(), errstr, sizeof(errstr));
  rd_kafka_conf_set(conf, "acks", "all", errstr, sizeof(errstr));
  rd_kafka_conf_set(conf, "enable.idempotence", conf_.kafka_enable_idempotence ? "true" : "false", errstr, sizeof(errstr));
  rd_kafka_conf_set(conf, "max.in.flight.requests.per.connection", "1", errstr, sizeof(errstr));
  if (conf_.kafka_message_max_bytes > 0) {
    std::string max_bytes = std::to_string(conf_.kafka_message_max_bytes);
    rd_kafka_conf_set(conf, "message.max.bytes", max_bytes.c_str(), errstr, sizeof(errstr));
  }

  rd_kafka_conf_set_dr_msg_cb(conf, &KafkaSender::DeliveryReportCallback);

  producer_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
  if (!producer_) {
    LOG(ERROR) << "Failed to create Kafka producer: " << errstr;
    return false;
  }
  return true;
}

void KafkaSender::CloseProducer() {
  if (!producer_) {
    return;
  }
  rd_kafka_flush(producer_, 10000);
  rd_kafka_destroy(producer_);
  producer_ = nullptr;
}

void KafkaSender::DeliveryReportCallback(rd_kafka_t* rk, const rd_kafka_message_t* rkmessage, void* opaque) {
  auto* ctx = static_cast<DeliveryContext*>(rkmessage->_private);
  if (ctx && ctx->sender) {
    if (rkmessage->err) {
      ctx->sender->ack_err_total_.fetch_add(1, std::memory_order_relaxed);
    } else {
      ctx->sender->ack_total_.fetch_add(1, std::memory_order_relaxed);
    }
  }
  if (rkmessage->err) {
    if (ctx) {
      LOG(WARNING) << "Kafka delivery failed: " << rd_kafka_err2str(rkmessage->err)
                   << " key=" << FormatKeyForLog(ctx->key)
                   << " payload_size=" << ctx->payload_size;
    } else {
      LOG(WARNING) << "Kafka delivery failed: " << rd_kafka_err2str(rkmessage->err)
                   << " payload_size=" << rkmessage->len;
    }
  } else if (ctx && ctx->has_checkpoint && ctx->checkpoint_manager) {
    ctx->checkpoint_manager->OnAck(rk, ctx->checkpoint);
  }
  delete ctx;
}

void* KafkaSender::ThreadMain() {
  LOG(INFO) << "Start KafkaSender " << id_ << " thread...";
  if (!InitProducer()) {
    return nullptr;
  }

  const bool stats_enabled =
      conf_.kafka_stats_mode != KafkaStatsMode::kNone && conf_.heartbeat_interval_ms > 0;
  const auto stats_interval = std::chrono::milliseconds(conf_.heartbeat_interval_ms);
  auto last_outq_sample = std::chrono::steady_clock::now() - stats_interval;
  auto maybe_sample_outq = [&](const std::chrono::steady_clock::time_point& now) {
    if (!stats_enabled) {
      return;
    }
    if (now - last_outq_sample < stats_interval) {
      return;
    }
    int outq = producer_ ? rd_kafka_outq_len(producer_) : 0;
    if (outq < 0) {
      outq = 0;
    }
    outq_len_.store(outq, std::memory_order_relaxed);
    last_outq_sample = now;
  };
  while (!should_exit_) {
    KafkaRecord record;
    {
      std::unique_lock lock(queue_mutex_);
      queue_signal_.wait_for(lock, std::chrono::milliseconds(100),
                             [this]() { return should_exit_.load() || !queue_.empty(); });
      if (queue_.empty()) {
        rd_kafka_poll(producer_, 0);
        maybe_sample_outq(std::chrono::steady_clock::now());
        continue;
      }
      record = queue_.front();
      queue_.pop();
      queue_size_.fetch_sub(1, std::memory_order_relaxed);
    }

    elements_++;
    send_total_.fetch_add(1, std::memory_order_relaxed);
    auto* ctx = new DeliveryContext{checkpoint_manager_,
                                    record.checkpoint,
                                    record.has_checkpoint,
                                    record.key,
                                    record.payload.size(),
                                    this};
    rd_kafka_resp_err_t err = rd_kafka_producev(
        producer_,
        RD_KAFKA_V_TOPIC(record.topic.c_str()),
        RD_KAFKA_V_KEY(record.key.data(), record.key.size()),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_VALUE(record.payload.data(), record.payload.size()),
        RD_KAFKA_V_OPAQUE(ctx),
        RD_KAFKA_V_END);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      produce_err_total_.fetch_add(1, std::memory_order_relaxed);
      LOG(WARNING) << "Kafka produce failed: " << rd_kafka_err2str(err)
                   << " key=" << FormatKeyForLog(record.key)
                   << " payload_size=" << record.payload.size();
      delete ctx;
    }

    rd_kafka_poll(producer_, 0);
    maybe_sample_outq(std::chrono::steady_clock::now());
  }

  rd_kafka_poll(producer_, 0);
  CloseProducer();
  LOG(INFO) << "KafkaSender " << id_ << " thread complete";
  return nullptr;
}
