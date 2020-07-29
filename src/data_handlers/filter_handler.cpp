#include <iostream>
#include <utility>

#include <arrow/compute/api.h>
#include <gandiva/selection_vector.h>
#include <gandiva/tree_expr_builder.h>

#include "filter_handler.h"
#include "utils/serializer.h"

FilterHandler::FilterHandler(std::shared_ptr<arrow::Schema> schema,
                             const std::vector<gandiva::ConditionPtr> &conditions) : schema_(std::move(schema)) {
  if (conditions.empty()) {
    throw std::runtime_error("Expected at least one condition for filter");
  }

  if (conditions.size() > 1) {
    gandiva::NodeVector conditions_nodes;
    for (auto &condition : conditions) {
      conditions_nodes.push_back(condition->root());
    }

    auto and_condition = gandiva::TreeExprBuilder::MakeCondition(gandiva::TreeExprBuilder::MakeAnd(conditions_nodes));
    auto filter_status = gandiva::Filter::Make(schema_, and_condition, &filter_);
    if (!filter_status.ok()) {
      throw std::runtime_error(filter_status.ToString());
    }
  } else {
    auto filter_status = gandiva::Filter::Make(schema_, conditions.back(), &filter_);
    if (!filter_status.ok()) {
      throw std::runtime_error(filter_status.ToString());
    }
  }
}

arrow::Status FilterHandler::handle(const std::shared_ptr<arrow::Buffer> &source, std::shared_ptr<arrow::Buffer> *target) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  ARROW_RETURN_NOT_OK(Serializer::deserializeRecordBatches(source, &record_batches));

  auto pool = arrow::default_memory_pool();
  arrow::RecordBatchVector result_record_batches;
  for (auto& record_batch : record_batches) {
    std::shared_ptr<gandiva::SelectionVector> selection;
    ARROW_RETURN_NOT_OK(gandiva::SelectionVector::MakeInt64(record_batch->num_rows(), pool, &selection));
    ARROW_RETURN_NOT_OK(filter_->Evaluate(*record_batch, selection));
    auto take_result = arrow::compute::Take(record_batch, selection->ToArray());
    if (!take_result.ok()) {
      return take_result.status();
    }

    result_record_batches.push_back(take_result.ValueOrDie().record_batch());
  }

  ARROW_RETURN_NOT_OK(Serializer::serializeRecordBatches(schema_, result_record_batches, target));
  return arrow::Status::OK();
}
