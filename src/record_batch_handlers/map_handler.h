#pragma once

#include <memory>

#include <arrow/api.h>

#include <gandiva/expression.h>
#include <gandiva/projector.h>

#include "record_batch_handler.h"

#include "metadata.pb.h"

class MapHandler : public RecordBatchHandler {
 public:
  struct MapCase {
    gandiva::ExpressionPtr expression;
    ColumnType result_column_type{FIELD};
  };

  explicit MapHandler(const std::vector<MapCase>& map_cases);

  arrow::Status handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch,
      arrow::RecordBatchVector* result) override;

 private:
  static arrow::Status eval(
      std::shared_ptr<arrow::RecordBatch>* record_batch,
      const std::shared_ptr<gandiva::Projector>& projector,
      const std::shared_ptr<arrow::Schema>& result_schema);

  arrow::Status prepareProjector(
      const std::shared_ptr<arrow::Schema>& input_schema,
      std::shared_ptr<gandiva::Projector>* projector) const;

  arrow::Status prepareResultSchema(
      const std::shared_ptr<arrow::Schema>& input_schema,
      std::shared_ptr<arrow::Schema>* result_schema) const;

 private:
  gandiva::ExpressionVector expressions_;
  std::vector<ColumnType> column_types_;
};
