#include <vector>

#include "map_handler.h"

#include "utils/serializer.h"

arrow::Status MapHandler::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
    arrow::RecordBatchVector* result) {
  auto result_record_batch = arrow::RecordBatch::Make(
      record_batch->schema(),
      record_batch->num_rows(),
      record_batch->columns()
      );

  std::shared_ptr<gandiva::Projector> projector;
  ARROW_RETURN_NOT_OK(prepareProjector(result_record_batch->schema(), &projector));
  auto result_schema = prepareResultSchema(result_record_batch->schema());
  ARROW_RETURN_NOT_OK(eval(&result_record_batch, projector, result_schema));

  copyMetadata(record_batch, &result_record_batch);
  result->push_back(result_record_batch);

  return arrow::Status::OK();
}

arrow::Status MapHandler::prepareProjector(
    const std::shared_ptr<arrow::Schema>& input_schema,
    std::shared_ptr<gandiva::Projector>* projector) const {
  ARROW_RETURN_NOT_OK(
      gandiva::Projector::Make(input_schema, expressions_, projector));
  return arrow::Status::OK();
}

std::shared_ptr<arrow::Schema> MapHandler::prepareResultSchema(
    const std::shared_ptr<arrow::Schema>& input_schema) const {
  arrow::FieldVector result_fields;
  for (auto& input_field : input_schema->fields()) {
    result_fields.push_back(input_field);
  }

  for (auto& expression : expressions_) {
    result_fields.push_back(expression->result());
  }

  return arrow::schema(result_fields);
}

arrow::Status MapHandler::eval(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const std::shared_ptr<gandiva::Projector>& projector,
    const std::shared_ptr<arrow::Schema>& result_schema) {
  auto input_schema_size = record_batch->get()->schema()->num_fields();
  auto pool = arrow::default_memory_pool();
  arrow::ArrayVector result_arrays;

  ARROW_RETURN_NOT_OK(
      projector->Evaluate(**record_batch, pool, &result_arrays));

  for (int i = 0; i < result_arrays.size(); ++i) {
    auto add_column_result = record_batch->get()->AddColumn(
        input_schema_size + i, result_schema->field(input_schema_size + i),
        result_arrays[i]);

    ARROW_RETURN_NOT_OK(add_column_result.status());
    *record_batch = add_column_result.ValueOrDie();
  }

  return arrow::Status::OK();
}
