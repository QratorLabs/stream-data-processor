#pragma once

#include <ostream>

#include "request_handler.h"

namespace stream_data_processor {
namespace kapacitor_udf {

class LoggingRequestHandler : public StreamRequestHandlerBase {
 public:
  explicit LoggingRequestHandler(const IUDFAgent* agent,
                                 std::ostream& logging_stream,
                                 bool enable_logging = true);

  [[nodiscard]] agent::Response info() const override;
  [[nodiscard]] agent::Response init(
      const agent::InitRequest& init_request) override;
  [[nodiscard]] agent::Response snapshot() const override;
  [[nodiscard]] agent::Response restore(
      const agent::RestoreRequest& restore_request) override;
  void point(const agent::Point& point) override;

 private:
  std::ostream& logging_stream_;
  bool is_logging_;
};

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
