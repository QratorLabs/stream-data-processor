#include <iostream>
#include <utility>
#include <vector>

#include <arrow/csv/api.h>

#include "finalize_node.h"

FinalizeNode::FinalizeNode(std::shared_ptr<uvw::Loop> loop, const IPv4Endpoint &listen_endpoint, std::ofstream& ostrm)
    : loop_(std::move(loop))
    , server_(loop_->resource<uvw::TCPHandle>())
    , buffer_builder_(std::make_shared<arrow::BufferBuilder>())
    , ostrm_(ostrm) {
  configureServer(listen_endpoint);
}

void FinalizeNode::configureServer(const IPv4Endpoint &endpoint) {
  server_->once<uvw::ListenEvent>([this](const uvw::ListenEvent& event, uvw::TCPHandle& server) {
    auto client = server.loop().resource<uvw::TCPHandle>();

    client->on<uvw::DataEvent>([this](const uvw::DataEvent& event, uvw::TCPHandle& client) {
      std::cerr << "Data received, length: " << event.length << std::endl;
      buffer_builder_->Append(event.data.get(), event.length);
    });

    client->once<uvw::ErrorEvent>([this](const uvw::ErrorEvent& event, uvw::TCPHandle& client) {
      writeData();
      stop();
      client.close();
    });

    client->once<uvw::EndEvent>([this](const uvw::EndEvent& event, uvw::TCPHandle& client) {
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
    return;
  }

  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  if (!Utils::deserializeRecordBatches(buffer, &record_batches).ok()) {
    return;
  }

  for (auto& record_batch : record_batches) {
    ostrm_ << record_batch->ToString() << std::endl;
  }
}

void FinalizeNode::stop() {
  server_->close();
}
