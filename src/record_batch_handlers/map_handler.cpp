#include <vector>

#include "map_handler.h"

#include "utils/serializer.h"

MapHandler::MapHandler(const std::vector<MapCase>& map_cases) {
  for (auto& map_case : map_cases) {
    expressions_.push_back(map_case.expression);
    column_types_.push_back(map_case.result_column_type);
  }
}

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
  std::shared_ptr<arrow::Schema> result_schema = nullptr;

  ARROW_RETURN_NOT_OK(prepareResultSchema(
      result_record_batch->schema(), &result_schema));

  ARROW_RETURN_NOT_OK(eval(&result_record_batch, projector, result_schema));

  copySchemaMetadata(record_batch, &result_record_batch);
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

arrow::Status MapHandler::prepareResultSchema(
    const std::shared_ptr<arrow::Schema>& input_schema,
    std::shared_ptr<arrow::Schema>* result_schema) const {
  arrow::FieldVector result_fields;
  for (auto& input_field : input_schema->fields()) {
    result_fields.push_back(input_field);
  }

  for (size_t i = 0; i < expressions_.size(); ++i) {
    result_fields.push_back(expressions_[i]->result());
    ARROW_RETURN_NOT_OK(ColumnTyping::setColumnTypeMetadata(
        &result_fields.back(), column_types_[i]));
  }

  *result_schema = arrow::schema(result_fields);
  return arrow::Status::OK();
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
