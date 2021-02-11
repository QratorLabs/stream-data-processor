#include <vector>

#include "map_handler.h"

#include "utils/serialize_utils.h"

namespace stream_data_processor {

MapHandler::MapHandler(const std::vector<MapCase>& map_cases) {
  for (auto& map_case : map_cases) {
    expressions_.push_back(map_case.expression);
    column_types_.push_back(map_case.result_column_type);
  }
}

arrow::Result<arrow::RecordBatchVector> MapHandler::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  auto result_record_batch = arrow::RecordBatch::Make(
      record_batch->schema(), record_batch->num_rows(),
      record_batch->columns());

  std::shared_ptr<gandiva::Projector> projector;
  ARROW_RETURN_NOT_OK(gandiva::Projector::Make(result_record_batch->schema(),
                                               expressions_, &projector));

  ARROW_ASSIGN_OR_RAISE(auto result_schema,
                        createResultSchema(result_record_batch->schema()));

  ARROW_RETURN_NOT_OK(eval(&result_record_batch, projector, result_schema));

  copySchemaMetadata(*record_batch, &result_record_batch);
  return arrow::RecordBatchVector{result_record_batch};
}

arrow::Result<std::shared_ptr<arrow::Schema>> MapHandler::createResultSchema(
    const std::shared_ptr<arrow::Schema>& input_schema) const {
  arrow::FieldVector result_fields;
  for (auto& input_field : input_schema->fields()) {
    result_fields.push_back(input_field);
  }

  for (size_t i = 0; i < expressions_.size(); ++i) {
    result_fields.push_back(expressions_[i]->result());
    ARROW_RETURN_NOT_OK(metadata::setColumnTypeMetadata(&result_fields.back(),
                                                        column_types_[i]));
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
    ARROW_ASSIGN_OR_RAISE(
        *record_batch,
        record_batch->get()->AddColumn(
            input_schema_size + i,
            result_schema->field(input_schema_size + i), result_arrays[i]));
  }

  return arrow::Status::OK();
}

}  // namespace stream_data_processor
