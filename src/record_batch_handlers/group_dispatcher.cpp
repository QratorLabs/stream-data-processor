#include "group_dispatcher.h"
#include "metadata/grouping.h"

namespace stream_data_processor {

GroupDispatcher::GroupDispatcher(
    std::shared_ptr<HandlerFactory> handler_factory)
    : handler_factory_(std::move(handler_factory)) {}

arrow::Result<arrow::RecordBatchVector> GroupDispatcher::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  auto group_metadata = metadata::extractGroupMetadata(*record_batch);

  if (groups_states_.find(group_metadata) == groups_states_.end()) {
    groups_states_[group_metadata] = handler_factory_->createHandler();
  }

  return groups_states_[group_metadata]->handle(record_batch);
}

}  // namespace stream_data_processor
