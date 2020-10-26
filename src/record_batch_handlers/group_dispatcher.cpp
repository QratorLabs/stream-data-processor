#include "group_dispatcher.h"
#include "grouping/grouping.h"

GroupDispatcher::GroupDispatcher(
    std::shared_ptr<StatefulHandler> initial_state)
    : initial_state_(std::move(initial_state)) {

}

arrow::Status GroupDispatcher::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
    arrow::RecordBatchVector* result
    ) {
  auto group_metadata =
      RecordBatchGrouping::extractGroupMetadata(record_batch);

  if (groups_states_.find(group_metadata) == groups_states_.end()) {
    groups_states_[group_metadata] = initial_state_->clone();
  }

  ARROW_RETURN_NOT_OK(
      groups_states_[group_metadata]->handle(record_batch, result));

  return arrow::Status::OK();
}
