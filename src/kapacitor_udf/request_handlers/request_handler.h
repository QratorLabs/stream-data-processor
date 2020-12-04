#pragma once

#include <memory>

#include "kapacitor_udf/udf_agent.h"

#include "udf.pb.h"

namespace stream_data_processor {
namespace kapacitor_udf {

class IUDFAgent;

class RequestHandler {
 public:
  explicit RequestHandler(const std::shared_ptr<IUDFAgent>& agent)
      : agent_(agent) {}

  ~RequestHandler() = default;

  RequestHandler(const RequestHandler&) = delete;
  RequestHandler& operator=(const RequestHandler&) = delete;

  RequestHandler(RequestHandler&&) = delete;
  RequestHandler& operator=(RequestHandler&&) = delete;

  [[nodiscard]] virtual agent::Response info() const = 0;
  [[nodiscard]] virtual agent::Response init(
      const agent::InitRequest& init_request) = 0;
  [[nodiscard]] virtual agent::Response snapshot() const = 0;
  [[nodiscard]] virtual agent::Response restore(
      const agent::RestoreRequest& restore_request) = 0;
  virtual void beginBatch(const agent::BeginBatch& batch) = 0;
  virtual void point(const agent::Point& point) = 0;
  virtual void endBatch(const agent::EndBatch& batch) = 0;

  virtual void start() {}
  virtual void stop() {}

 protected:
  std::shared_ptr<IUDFAgent> getAgent() const { return agent_.lock(); }

 private:
  std::weak_ptr<IUDFAgent> agent_;
};

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
