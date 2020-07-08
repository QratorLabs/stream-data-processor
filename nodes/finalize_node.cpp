#include <iostream>
#include <vector>

#include <arrow/csv/api.h>
#include <arrow/io/api.h>

#include "finalize_node.h"

FinalizeNode::FinalizeNode(const IPv4Endpoint &listen_endpoint, std::ofstream& ostrm)
    : loop_(uvw::Loop::getDefault())
    , server_(loop_->resource<uvw::TCPHandle>())
    , buffer_builder_(std::make_shared<arrow::BufferBuilder>())
    , ostrm_(ostrm) {
  configureServer(listen_endpoint);
}

void FinalizeNode::configureServer(const IPv4Endpoint &endpoint) {
  server_->once<uvw::ListenEvent>([this](const uvw::ListenEvent& event, uvw::TCPHandle& server) {
    std::cerr << "New connection!" << std::endl;
    auto client = server.loop().resource<uvw::TCPHandle>();

    client->on<uvw::DataEvent>([this](const uvw::DataEvent& event, uvw::TCPHandle& client) {
      std::cerr << "Data received: " << event.data.get() << std::endl;
      buffer_builder_->Append(event.data.get(), event.length);
    });

    client->once<uvw::ErrorEvent>([this](const uvw::ErrorEvent& event, uvw::TCPHandle& client) {
      std::cerr << event.what() << std::endl;
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
    std::cerr << "No data to write" << std::endl;
    return;
  }

  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  if (!Utils::BufferToRecordBatches(buffer, &record_batches).ok()) {
    std::cerr << "Error while reading Record Batches from Buffer" << std::endl;
  }

  for (auto& record_batch : record_batches) {
    ostrm_ << record_batch->ToString() << std::endl;
  }
}

void FinalizeNode::stop() {
  server_->close();
  ostrm_.close();
}

void FinalizeNode::start() {
  loop_->run();
}
