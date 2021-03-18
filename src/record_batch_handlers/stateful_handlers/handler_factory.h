#pragma once

#include <memory>

#include "record_batch_handlers/record_batch_handler.h"

namespace stream_data_processor {

class HandlerFactory {
 public:
  virtual std::shared_ptr<RecordBatchHandler> createHandler() const = 0;

  virtual ~HandlerFactory() = 0;

 protected:
  HandlerFactory() = default;

  HandlerFactory(const HandlerFactory& /* non-used */) = default;
  HandlerFactory& operator=(const HandlerFactory& /* non-used */) = default;

  HandlerFactory(HandlerFactory&& /* non-used */) = default;
  HandlerFactory& operator=(HandlerFactory&& /* non-used */) = default;
};

}  // namespace stream_data_processor
