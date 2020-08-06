#include <utility>
#include <vector>

#include <arrow/api.h>

#include <spdlog/spdlog.h>

#include <bprinter/table_printer.h>

#include "print_node.h"
#include "utils/utils.h"

PrintNode::PrintNode(std::string name,
                     const std::shared_ptr<uvw::Loop>& loop,
                     TransportUtils::Subscriber&& subscriber,
                     std::ofstream& ostrm)
    : NodeBase(std::move(name), loop, std::move(subscriber))
    , buffer_builder_(std::make_shared<arrow::BufferBuilder>())
    , ostrm_(ostrm) {
  configureServer();
}

void PrintNode::configureServer() {
  poller_->on<uvw::PollEvent>([this](const uvw::PollEvent& event, uvw::PollHandle& poller) {
    if (subscriber_.subscriber_socket().getsockopt<int>(ZMQ_EVENTS) & ZMQ_POLLIN) {
      auto message = readMessage();
      if (!subscriber_.isReady()) {
        spdlog::get(name_)->info("Connected to publisher");
        subscriber_.confirmConnection();
      } else if (message.to_string() != TransportUtils::CONNECT_MESSAGE) {
        auto append_status = appendData(static_cast<const char *>(message.data()), message.size());
      }

      writeData();
    }

    if (event.flags & uvw::PollHandle::Event::DISCONNECT) {
      spdlog::get(name_)->info("Closed connection with publisher");
      writeData();
      stop();
    }
  });

  poller_->start(uvw::Flags<uvw::PollHandle::Event>::from<uvw::PollHandle::Event::READABLE, uvw::PollHandle::Event::DISCONNECT>());
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
  poller_->close();
  subscriber_.subscriber_socket().close();
  subscriber_.synchronize_socket().close();
}

void PrintNode::writeRecordBatch(const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  bprinter::TablePrinter table_printer(&ostrm_);
  for (auto& field : record_batch->schema()->fields()) {
    switch (field->type()->id()) {
      case arrow::Type::INT64:
        table_printer.AddColumn(field->name(), 15);
        break;
      case arrow::Type::DOUBLE:
        table_printer.AddColumn(field->name(), 20);
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

arrow::Status PrintNode::appendData(const char *data, size_t length) {
  spdlog::get(name_)->debug("Appending data of size {} to buffer", length);
  ARROW_RETURN_NOT_OK(buffer_builder_->Append(data, length));
  return arrow::Status::OK();
}
