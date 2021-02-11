#include <arrow/compute/api.h>
#include <gandiva/selection_vector.h>
#include <gandiva/tree_expr_builder.h>

#include "filter_handler.h"
#include "utils/serialize_utils.h"

namespace stream_data_processor {

arrow::Result<arrow::RecordBatchVector> FilterHandler::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  auto pool = arrow::default_memory_pool();
  ARROW_ASSIGN_OR_RAISE(auto filter, createFilter(record_batch->schema()));

  std::shared_ptr<gandiva::SelectionVector> selection;
  ARROW_RETURN_NOT_OK(gandiva::SelectionVector::MakeInt64(
      record_batch->num_rows(), pool, &selection));

  ARROW_RETURN_NOT_OK(filter->Evaluate(*record_batch, selection));

  arrow::Datum take_datum;
  ARROW_ASSIGN_OR_RAISE(
      take_datum, arrow::compute::Take(record_batch, selection->ToArray()));

  auto result_record_batch = take_datum.record_batch();
  copySchemaMetadata(*record_batch, &result_record_batch);
  ARROW_RETURN_NOT_OK(copyColumnTypes(*record_batch, &result_record_batch));

  return arrow::RecordBatchVector{result_record_batch};
}

arrow::Result<std::shared_ptr<gandiva::Filter>> FilterHandler::createFilter(
    const std::shared_ptr<arrow::Schema>& schema) const {
  if (conditions_.empty()) {
    return arrow::Status::Invalid(
        "Expected at least one condition for filter");
  }

  std::shared_ptr<gandiva::Filter> filter;
  if (conditions_.size() > 1) {
    gandiva::NodeVector conditions_nodes;
    for (auto& condition : conditions_) {
      conditions_nodes.push_back(condition->root());
    }

    auto and_condition = gandiva::TreeExprBuilder::MakeCondition(
        gandiva::TreeExprBuilder::MakeAnd(conditions_nodes));

    ARROW_RETURN_NOT_OK(
        gandiva::Filter::Make(schema, and_condition, &filter));
  } else {
    ARROW_RETURN_NOT_OK(
        gandiva::Filter::Make(schema, conditions_.back(), &filter));
  }

  return filter;
}

}  // namespace stream_data_processor
