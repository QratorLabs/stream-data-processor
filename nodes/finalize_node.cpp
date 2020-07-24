#include <utility>
#include <vector>

#include <arrow/api.h>

#include <spdlog/spdlog.h>

#include "finalize_node.h"
#include "utils/utils.h"

FinalizeNode::FinalizeNode(std::string name,
    const std::shared_ptr<uvw::Loop>& loop,
    const IPv4Endpoint &listen_endpoint,
    std::ofstream& ostrm)
    : NodeBase(std::move(name), loop, listen_endpoint)
    , buffer_builder_(std::make_shared<arrow::BufferBuilder>())
    , ostrm_(ostrm) {
  configureServer();
}

void FinalizeNode::configureServer() {
  server_->once<uvw::ListenEvent>([this](const uvw::ListenEvent& event, uvw::TCPHandle& server) {
    spdlog::get(name_)->info("New client connection");

    auto client = server.loop().resource<uvw::TCPHandle>();

    client->on<uvw::DataEvent>([this](const uvw::DataEvent& event, uvw::TCPHandle& client) {
      spdlog::get(name_)->debug("Data received, size: {}", event.length);
      for (auto& data_part : NetworkUtils::splitMessage(event.data.get(), event.length)) {
        auto append_status = buffer_builder_->Append(data_part.first, data_part.second);
        if (!append_status.ok()) {
          spdlog::get(name_)->error(append_status.ToString());
        }

        writeData();
      }
    });

    client->once<uvw::ErrorEvent>([this](const uvw::ErrorEvent& event, uvw::TCPHandle& client) {
      spdlog::get(name_)->error(event.what());
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
}

void FinalizeNode::writeData() {
  std::shared_ptr<arrow::Buffer> buffer;
  if (!buffer_builder_->Finish(&buffer).ok() || buffer->size() == 0) {
    spdlog::get(name_)->debug("No data to write");
    return;
  }

  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  auto deserialize_status = Serializer::deserializeRecordBatches(buffer, &record_batches);
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
