#pragma once

#include "period_handler.h"

class WindowHandler : public PeriodHandler {
 public:
  arrow::Status handle(const std::deque<std::shared_ptr<arrow::Buffer>> &period,
                       std::shared_ptr<arrow::Buffer> &result) override;
};


