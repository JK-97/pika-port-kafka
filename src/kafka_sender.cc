#include "kafka_sender.h"

#include <chrono>
#include <utility>

#include <glog/logging.h>

KafkaSender::KafkaSender(int id, const Conf& conf, CheckpointManager* checkpoint_manager)
    : id_(id), conf_(conf), checkpoint_manager_(checkpoint_manager), producer_(nullptr), should_exit_(false), elements_(0) {}

KafkaSender::~KafkaSender() {
  Stop();
  LOG(INFO) << "KafkaSender thread " << id_ << " exit!!!";
}

void KafkaSender::Enqueue(const KafkaRecord& record) {
  std::unique_lock lock(queue_mutex_);
  if (queue_.size() < 100000) {
    queue_.push(record);
    queue_signal_.notify_one();
    return;
  }

  queue_signal_.wait(lock, [this] { return queue_.size() <= 100000 || should_exit_.load(); });
  if (should_exit_) {
    return;
  }
  queue_.push(record);
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
  if (rkmessage->err) {
    LOG(WARNING) << "Kafka delivery failed: " << rd_kafka_err2str(rkmessage->err);
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

  while (!should_exit_) {
    KafkaRecord record;
    {
      std::unique_lock lock(queue_mutex_);
      queue_signal_.wait_for(lock, std::chrono::milliseconds(100),
                             [this]() { return should_exit_.load() || !queue_.empty(); });
      if (queue_.empty()) {
        rd_kafka_poll(producer_, 0);
        continue;
      }
      record = queue_.front();
      queue_.pop();
    }

    elements_++;
    auto* ctx = new DeliveryContext{checkpoint_manager_, record.checkpoint, record.has_checkpoint};
    rd_kafka_resp_err_t err = rd_kafka_producev(
        producer_,
        RD_KAFKA_V_TOPIC(record.topic.c_str()),
        RD_KAFKA_V_KEY(record.key.data(), record.key.size()),
        RD_KAFKA_V_VALUE(record.payload.data(), record.payload.size()),
        RD_KAFKA_V_OPAQUE(ctx),
        RD_KAFKA_V_END);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      LOG(WARNING) << "Kafka produce failed: " << rd_kafka_err2str(err);
      delete ctx;
    }

    rd_kafka_poll(producer_, 0);
  }

  rd_kafka_poll(producer_, 0);
  CloseProducer();
  LOG(INFO) << "KafkaSender " << id_ << " thread complete";
  return nullptr;
}
