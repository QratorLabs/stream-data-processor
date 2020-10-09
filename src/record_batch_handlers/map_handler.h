#pragma once

#include <memory>

#include <arrow/api.h>

#include <gandiva/expression.h>
#include <gandiva/projector.h>

#include "record_batch_handler.h"

class MapHandler : public RecordBatchHandler {
 public:
  template <typename ExpressionVectorType>
  explicit MapHandler(ExpressionVectorType&& expressions)
      : expressions_(std::forward<ExpressionVectorType>(expressions)) {

  }

  arrow::Status handle(const arrow::RecordBatchVector& record_batches, arrow::RecordBatchVector* result) override;

 private:
  static arrow::Status eval(arrow::RecordBatchVector* record_batches,
                     const std::shared_ptr<gandiva::Projector>& projector,
                     const std::shared_ptr<arrow::Schema>& result_schema) ;

  arrow::Status prepareProjector(const std::shared_ptr<arrow::Schema>& input_schema,
                                 std::shared_ptr<gandiva::Projector>* projector) const;

  [[nodiscard]] std::shared_ptr<arrow::Schema> prepareResultSchema(const std::shared_ptr<arrow::Schema>& input_schema) const;

 private:
  gandiva::ExpressionVector expressions_;
};
