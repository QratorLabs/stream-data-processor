#include <arrow/compute/api.h>

#include "sorter.h"

#include "utils.h"

Sorter::Sorter(std::vector<std::string> column_names) : column_names_(std::move(column_names)) {

}

arrow::Status Sorter::handle(std::shared_ptr<arrow::Buffer> source, std::shared_ptr<arrow::Buffer> *target) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  ARROW_RETURN_NOT_OK(Utils::deserializeRecordBatches(source, &record_batches));
  if (record_batches.empty()) {
    return arrow::Status::CapacityError("No data to sort");
  }

  std::vector<std::shared_ptr<arrow::RecordBatch>> sorted_record_batches;
  auto pool = arrow::default_memory_pool();
  for (auto& record_batch : record_batches) {
    ARROW_RETURN_NOT_OK(sort(0, record_batch, sorted_record_batches));
  }

  ARROW_RETURN_NOT_OK(Utils::serializeRecordBatches(*sorted_record_batches.front()->schema(), sorted_record_batches, target));
  return arrow::Status::OK();
}

arrow::Status Sorter::sortByColumn(size_t i,
                                   const std::shared_ptr<arrow::RecordBatch>& source,
                                   std::shared_ptr<arrow::RecordBatch> *target) const {
  auto sorting_column = source->GetColumnByName(column_names_[i]);
  auto sorted_idx_result = arrow::compute::SortToIndices(*sorting_column);
  if (!sorted_idx_result.ok()) {
    return sorted_idx_result.status();
  }

  auto sorted_record_batch_result = arrow::compute::Take(source, sorted_idx_result.ValueOrDie());
  if (!sorted_record_batch_result.ok()) {
    return sorted_record_batch_result.status();
  }

  *target = sorted_record_batch_result.ValueOrDie().record_batch();
  return arrow::Status::OK();
}

arrow::Status Sorter::sort(size_t i,
                           const std::shared_ptr<arrow::RecordBatch>& source,
                           std::vector<std::shared_ptr<arrow::RecordBatch>> &targets) const {
  std::shared_ptr<arrow::RecordBatch> sorted_batch;
  ARROW_RETURN_NOT_OK(sortByColumn(i, source, &sorted_batch));
  if (i == column_names_.size() - 1) {
    targets.push_back(sorted_batch);
    return arrow::Status::OK();
  }

  while (true) {
    auto sorted_keys = sorted_batch->GetColumnByName(column_names_[i]);

    auto min_val1 = sorted_keys->GetScalar(0);
    if (!min_val1.ok()) {
      return min_val1.status();
    }

    auto max_val1 = sorted_keys->GetScalar(sorted_keys->length() - 1);
    if (!max_val1.ok()) {
      return max_val1.status();
    }

    auto equals_result = arrow::compute::Compare(sorted_keys, min_val1.ValueOrDie(),
                                                 arrow::compute::CompareOptions(arrow::compute::CompareOperator::EQUAL));
    if (!equals_result.ok()) {
      return equals_result.status();
    }

    auto filter_equals_result = arrow::compute::Filter(sorted_batch, equals_result.ValueOrDie());
    if (!filter_equals_result.ok()) {
      return filter_equals_result.status();
    }

    ARROW_RETURN_NOT_OK(sort(i + 1, filter_equals_result.ValueOrDie().record_batch(), targets));
    if (min_val1.ValueOrDie()->Equals(max_val1.ValueOrDie())) {
      break;
    }

    auto not_equals_result = arrow::compute::Compare(sorted_keys, min_val1.ValueOrDie(),
                                                 arrow::compute::CompareOptions(arrow::compute::CompareOperator::NOT_EQUAL));
    if (!not_equals_result.ok()) {
      return equals_result.status();
    }

    auto filter_not_equals_result = arrow::compute::Filter(sorted_batch, not_equals_result.ValueOrDie());
    if (!filter_not_equals_result.ok()) {
      return filter_not_equals_result.status();
    }

    sorted_batch = filter_not_equals_result.ValueOrDie().record_batch();
  }

  return arrow::Status::OK();
}
