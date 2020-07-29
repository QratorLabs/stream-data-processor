#pragma once

#include <memory>

#include <arrow/api.h>

#include <gandiva/expression.h>
#include <gandiva/projector.h>

#include "data_handler.h"

class MapHandler : public DataHandler {
 public:
  MapHandler(const std::shared_ptr<arrow::Schema>& input_schema, const gandiva::ExpressionVector& expressions);

  arrow::Status handle(const std::shared_ptr<arrow::Buffer> &source, std::shared_ptr<arrow::Buffer>* target) override;

 private:
  arrow::Status eval(arrow::RecordBatchVector& record_batches);

 private:
  std::shared_ptr<arrow::Schema> result_schema_;
  std::shared_ptr<gandiva::Projector> projector_;
};
