#include <iostream>
#include <vector>

#include "map_handler.h"

#include "utils.h"

MapHandler::MapHandler(const std::shared_ptr<arrow::Schema>& input_schema, const gandiva::ExpressionVector &expressions) {
  arrow::FieldVector result_fields;
  for (auto& input_field : input_schema->fields()) {
    result_fields.push_back(input_field);
  }
  for (auto& expression : expressions) {
    result_fields.push_back(expression->result());
  }
  result_schema_ = arrow::schema(result_fields);

  gandiva::Projector::Make(input_schema, expressions, &projector_);
}

arrow::Status MapHandler::handle(std::shared_ptr<arrow::Buffer> source, std::shared_ptr<arrow::Buffer> *target) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  ARROW_RETURN_NOT_OK(Utils::deserializeRecordBatches(source, &record_batches));
  ARROW_RETURN_NOT_OK(eval(record_batches));
  ARROW_RETURN_NOT_OK(Utils::serializeRecordBatches(*result_schema_, record_batches, target));
  return arrow::Status::OK();
}

arrow::Status MapHandler::eval(arrow::RecordBatchVector& record_batches) {
  if (record_batches.empty()) {
    return arrow::Status::CapacityError("No data to handle");
  }

  auto input_schema_size = record_batches.front()->schema()->num_fields();
  auto pool = arrow::default_memory_pool();
  for (auto& record_batch : record_batches) {
    arrow::ArrayVector result_arrays;
    ARROW_RETURN_NOT_OK(projector_->Evaluate(*record_batch, pool, &result_arrays));
    for (size_t i = 0; i < result_arrays.size(); ++i) {
      auto add_column_result = record_batch->AddColumn(
          input_schema_size + i,
          result_schema_->field(input_schema_size + i),
          result_arrays[i]
      );
      ARROW_RETURN_NOT_OK(add_column_result.status());
      record_batch = add_column_result.ValueOrDie();
    }
  }

  return arrow::Status::OK();
}
