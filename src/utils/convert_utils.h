#pragma once

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include <arrow/api.h>

namespace stream_data_processor {
namespace convert_utils {

arrow::Result<std::shared_ptr<arrow::RecordBatch>> convertTableToRecordBatch(
    const arrow::Table& table);

arrow::Result<std::shared_ptr<arrow::RecordBatch>> concatenateRecordBatches(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches);

template <typename ElementType>
inline void append(std::vector<ElementType>& from,
                   std::vector<ElementType>& to) {
  if (to.empty()) {
    to = from;
  } else {
    to.insert(to.end(), from.begin(), from.end());
  }
}

template <typename ElementType>
inline void append(std::vector<ElementType>&& from,
                   std::vector<ElementType>& to) {
  if (to.empty()) {
    to = std::move(from);
  } else {
    to.insert(to.end(), std::make_move_iterator(from.begin()),
              std::make_move_iterator(from.end()));
  }
}

}  // namespace convert_utils
}  // namespace stream_data_processor
