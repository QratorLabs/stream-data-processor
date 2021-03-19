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

class BatchRequestHandler : public BatchRecordBatchRequestHandlerBase {
 public:
  BatchRequestHandler(const IUDFAgent* agent,
                      std::unique_ptr<IPointsStorage>&& points_storage);

  [[nodiscard]] agent::Response info() const override;
  [[nodiscard]] agent::Response init(
      const agent::InitRequest& init_request) override;
};

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
