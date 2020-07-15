#include <utility>
#include <vector>

#include <arrow/api.h>

#include <spdlog/spdlog.h>

#include "finalize_node.h"

FinalizeNode::FinalizeNode(std::string name,
    std::shared_ptr<uvw::Loop> loop,
    const IPv4Endpoint &listen_endpoint, std::ofstream& ostrm)
    : name_(std::move(name))
    , logger_(spdlog::basic_logger_mt(name_, "logs/" + name_ + ".txt", true))
    , loop_(std::move(loop))
    , server_(loop_->resource<uvw::TCPHandle>())
    , buffer_builder_(std::make_shared<arrow::BufferBuilder>())
    , ostrm_(ostrm) {
  configureServer(listen_endpoint);
}

void FinalizeNode::configureServer(const IPv4Endpoint &endpoint) {
  server_->once<uvw::ListenEvent>([this](const uvw::ListenEvent& event, uvw::TCPHandle& server) {
    spdlog::get(name_)->info("New client connection");

    auto client = server.loop().resource<uvw::TCPHandle>();

    client->on<uvw::DataEvent>([this](const uvw::DataEvent& event, uvw::TCPHandle& client) {
      spdlog::get(name_)->debug("Data received, size: {}", event.length);
      buffer_builder_->Append(event.data.get(), event.length);
      writeData();
    });

    client->once<uvw::ErrorEvent>([this](const uvw::ErrorEvent& event, uvw::TCPHandle& client) {
      spdlog::get(name_)->error("Error: {}", event.what());
      writeData();
      stop();
      client.close();
    });

    client->once<uvw::EndEvent>([this](const uvw::EndEvent& event, uvw::TCPHandle& client) {
      spdlog::get(name_)->info("Closed connection with client");
      writeData();
      stop();
      client.close();
    });

    server.accept(*client);
    client->read();
  });

  server_->bind(endpoint.host, endpoint.port);
  server_->listen();
}

void FinalizeNode::writeData() {
  std::shared_ptr<arrow::Buffer> buffer;
  if (!buffer_builder_->Finish(&buffer).ok() || buffer->size() == 0) {
    spdlog::get(name_)->debug("No data to write");
    return;
  }

  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  auto deserialize_status = Utils::deserializeRecordBatches(buffer, &record_batches);
  if (!deserialize_status.ok()) {
    spdlog::get(name_)->debug(deserialize_status.ToString());
    return;
  }

  spdlog::get(name_)->info("Writing data");
  for (auto& record_batch : record_batches) {
    ostrm_ << record_batch->ToString() << std::endl;
  }
}

void FinalizeNode::stop() {
  spdlog::get(name_)->info("Stopping node");
  server_->close();
}
