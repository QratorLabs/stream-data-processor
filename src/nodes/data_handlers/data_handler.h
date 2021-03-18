#pragma once

#include <memory>
#include <vector>

#include <arrow/api.h>

namespace stream_data_processor {

class DataHandler {
 public:
  [[nodiscard]] virtual arrow::Result<arrow::BufferVector> handle(
      const arrow::Buffer& source) = 0;

  virtual ~DataHandler() = 0;

 protected:
  DataHandler() = default;

  DataHandler(const DataHandler& /* non-used */) = default;
  DataHandler& operator=(const DataHandler& /* non-used */) = default;

  DataHandler(DataHandler&& /* non-used */) = default;
  DataHandler& operator=(DataHandler&& /* non-used */) = default;
};

}  // namespace stream_data_processor
