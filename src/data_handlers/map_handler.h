#pragma once

#include <memory>

#include <arrow/api.h>

#include <gandiva/expression.h>
#include <gandiva/projector.h>

#include "data_handler.h"

class MapHandler : public DataHandler {
 public:
  explicit MapHandler(gandiva::ExpressionVector&& expressions);

  arrow::Status handle(const std::shared_ptr<arrow::Buffer> &source, std::shared_ptr<arrow::Buffer>* target) override;

 private:
  arrow::Status eval(arrow::RecordBatchVector &record_batches,
                     const std::shared_ptr<gandiva::Projector> &projector,
                     const std::shared_ptr<arrow::Schema> &result_schema) const;

  arrow::Status prepareProjector(const std::shared_ptr<arrow::Schema>& input_schema,
                                 std::shared_ptr<gandiva::Projector>& projector) const;

  [[nodiscard]] std::shared_ptr<arrow::Schema> prepareResultSchema(const std::shared_ptr<arrow::Schema>& input_schema) const;

 private:
  gandiva::ExpressionVector expressions_;
};
