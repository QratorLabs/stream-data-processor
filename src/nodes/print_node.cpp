#include <utility>
#include <vector>

#include <arrow/api.h>

#include <spdlog/spdlog.h>

#include <bprinter/table_printer.h>

#include "print_node.h"
#include "utils/utils.h"

PrintNode::PrintNode(std::string name,
                     const std::shared_ptr<uvw::Loop>& loop,
                     const IPv4Endpoint &listen_endpoint,
                     std::ofstream& ostrm)
    : NodeBase(std::move(name), loop, listen_endpoint)
    , buffer_builder_(std::make_shared<arrow::BufferBuilder>())
    , ostrm_(ostrm) {
  configureServer();
}

void PrintNode::configureServer() {
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

void PrintNode::writeData() {
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
    writeRecordBatch(record_batch);
    ostrm_ << std::endl;
  }
}

void PrintNode::stop() {
  spdlog::get(name_)->info("Stopping node");
  server_->close();
}

void PrintNode::writeRecordBatch(const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  bprinter::TablePrinter table_printer(&ostrm_);
  for (auto& field : record_batch->schema()->fields()) {
    switch (field->type()->id()) {
      case arrow::Type::INT64:
        table_printer.AddColumn(field->name(), 10);
        break;
      case arrow::Type::DOUBLE:
        table_printer.AddColumn(field->name(), 15);
        break;
      case arrow::Type::STRING:
        table_printer.AddColumn(field->name(), 25);
        break;
      default:
        table_printer.AddColumn(field->name(), 15);
    }
  }

  table_printer.PrintHeader();
  for (size_t i = 0; i < record_batch->num_rows(); ++i) {
    for (auto& column : record_batch->columns()) {
      auto value_result = column->GetScalar(i);
      if (!value_result.ok()) {
        spdlog::get(name_)->error(value_result.status().ToString());
      }

      table_printer << value_result.ValueOrDie()->ToString();
    }
  }

  table_printer.PrintFooter();
}
