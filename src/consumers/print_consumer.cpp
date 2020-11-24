#include <arrow/api.h>

#include <bprinter/table_printer.h>

#include "print_consumer.h"
#include "utils/serialize_utils.h"

namespace stream_data_processor {

PrintConsumer::PrintConsumer(std::ofstream& ostrm) : ostrm_(ostrm) {}

void PrintConsumer::start() {}

void PrintConsumer::consume(std::shared_ptr<arrow::Buffer> data) {
  auto record_batches = serialize_utils::deserializeRecordBatches(*data);
  if (!record_batches.ok()) {
    throw std::runtime_error(record_batches.status().message());
  }

  for (auto& record_batch : record_batches.ValueOrDie()) {
    printRecordBatch(*record_batch);
  }
}

void PrintConsumer::printRecordBatch(const arrow::RecordBatch& record_batch) {
  bprinter::TablePrinter table_printer(&ostrm_);
  for (auto& field : record_batch.schema()->fields()) {
    switch (field->type()->id()) {
      case arrow::Type::INT64:
        table_printer.AddColumn(field->name(), INT_COLUMN_WIDTH);
        break;
      case arrow::Type::DOUBLE:
        table_printer.AddColumn(field->name(), DOUBLE_COLUMN_WIDTH);
        break;
      case arrow::Type::STRING:
        table_printer.AddColumn(field->name(), STRING_COLUMN_WIDTH);
        break;
      default: table_printer.AddColumn(field->name(), DEFAULT_COLUMN_WIDTH);
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

void PrintConsumer::stop() {}

FilePrintConsumer::FilePrintConsumer(const std::string& file_name)
    : PrintConsumer(ostrm_obj_), ostrm_obj_(file_name) {}

FilePrintConsumer::~FilePrintConsumer() { ostrm_obj_.close(); }

}  // namespace stream_data_processor
