#include <arrow/compute/api.h>
#include <gandiva/selection_vector.h>
#include <gandiva/tree_expr_builder.h>

#include "filter_handler.h"
#include "utils/serializer.h"

arrow::Status FilterHandler::handle(const arrow::RecordBatchVector& record_batches, arrow::RecordBatchVector& result) {
  auto pool = arrow::default_memory_pool();
  std::shared_ptr<gandiva::Filter> filter;
  ARROW_RETURN_NOT_OK(prepareFilter(record_batches.back()->schema(), filter));
  for (auto& record_batch : record_batches) {
    std::shared_ptr<gandiva::SelectionVector> selection;
    ARROW_RETURN_NOT_OK(gandiva::SelectionVector::MakeInt64(record_batch->num_rows(), pool, &selection));
    ARROW_RETURN_NOT_OK(filter->Evaluate(*record_batch, selection));
    auto take_result = arrow::compute::Take(record_batch, selection->ToArray());
    if (!take_result.ok()) {
      return take_result.status();
    }

    result.push_back(take_result.ValueOrDie().record_batch());
  }

  return arrow::Status::OK();
}

arrow::Status FilterHandler::prepareFilter(const std::shared_ptr<arrow::Schema> &schema,
                                           std::shared_ptr<gandiva::Filter>& filter) const {
  if (conditions_.empty()) {
    return arrow::Status::Invalid("Expected at least one condition for filter");
  }

  if (conditions_.size() > 1) {
    gandiva::NodeVector conditions_nodes;
    for (auto &condition : conditions_) {
      conditions_nodes.push_back(condition->root());
    }

    auto and_condition = gandiva::TreeExprBuilder::MakeCondition(gandiva::TreeExprBuilder::MakeAnd(conditions_nodes));
    ARROW_RETURN_NOT_OK(gandiva::Filter::Make(schema, and_condition, &filter));
  } else {
    ARROW_RETURN_NOT_OK(gandiva::Filter::Make(schema, conditions_.back(), &filter));
  }

  return arrow::Status::OK();
}
