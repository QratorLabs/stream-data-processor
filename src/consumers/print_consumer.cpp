#include <arrow/api.h>

#include <bprinter/table_printer.h>

#include "print_consumer.h"
#include "utils/serializer.h"

PrintConsumer::PrintConsumer(std::ofstream& ostrm) : ostrm_(ostrm) {

}

void PrintConsumer::start() {

}

void PrintConsumer::consume(const char* data, size_t length) {
  auto buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(data), length);
  arrow::RecordBatchVector record_batches;
  auto deserialize_status = Serializer::deserializeRecordBatches(buffer, &record_batches);
  if (!deserialize_status.ok()) {
    throw std::runtime_error(deserialize_status.message());
  }

  for (auto& record_batch : record_batches) {
    printRecordBatch(*record_batch);
  }
}

void PrintConsumer::printRecordBatch(const arrow::RecordBatch& record_batch) {
  bprinter::TablePrinter table_printer(&ostrm_);
  for (auto& field : record_batch.schema()->fields()) {
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
  for (size_t i = 0; i < record_batch.num_rows(); ++i) {
    for (auto& column : record_batch.columns()) {
      auto value_result = column->GetScalar(i);
      if (!value_result.ok()) {
        throw std::runtime_error(value_result.status().message());
      }

      table_printer << value_result.ValueOrDie()->ToString();
    }
  }

  table_printer.PrintFooter();
  ostrm_ << std::endl;
}

void PrintConsumer::stop() {

}


FilePrintConsumer::FilePrintConsumer(const std::string& file_name)
    : PrintConsumer(ostrm_obj_), ostrm_obj_(file_name) {

}

FilePrintConsumer::~FilePrintConsumer() {
  ostrm_obj_.close();
}
