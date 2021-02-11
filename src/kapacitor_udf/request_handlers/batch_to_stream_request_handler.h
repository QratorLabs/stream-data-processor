#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "kapacitor_udf/udf_agent.h"
#include "kapacitor_udf/utils/points_converter.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "record_batch_request_handler.h"

#include "udf.pb.h"

namespace stream_data_processor {
namespace kapacitor_udf {

class BatchToStreamRequestHandler : public RecordBatchRequestHandler {
 public:
  BatchToStreamRequestHandler(const IUDFAgent* agent);

  [[nodiscard]] agent::Response info() const override;
  [[nodiscard]] agent::Response init(
      const agent::InitRequest& init_request) override;
  [[nodiscard]] agent::Response snapshot() const override;
  [[nodiscard]] agent::Response restore(
      const agent::RestoreRequest& restore_request) override;
  void beginBatch(const agent::BeginBatch& batch) override;
  void point(const agent::Point& point) override;
  void endBatch(const agent::EndBatch& batch) override;

 private:
  bool in_batch_{false};
};

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
