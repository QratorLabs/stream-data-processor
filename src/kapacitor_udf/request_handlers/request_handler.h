#pragma once

#include "udf.pb.h"

class RequestHandler {
 public:
  [[nodiscard]] virtual agent::Response info() const = 0;
  virtual agent::Response init(const agent::InitRequest& init_request) = 0;
  virtual agent::Response snapshot() = 0;
  virtual agent::Response restore(const agent::RestoreRequest& restore_request) = 0;
  virtual void beginBatch(const agent::BeginBatch& batch) = 0;
  virtual void point(const agent::Point& point) = 0;
  virtual void endBatch(const agent::EndBatch& batch) = 0;
};
