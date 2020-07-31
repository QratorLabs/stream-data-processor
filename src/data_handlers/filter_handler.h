#pragma once

#include <memory>

#include <gandiva/condition.h>
#include <gandiva/filter.h>

#include "data_handler.h"

class FilterHandler : public DataHandler {
 public:
  template <typename U>
  explicit FilterHandler(U&& conditions)
      : conditions_(std::forward<U>(conditions)) {

  }

  arrow::Status handle(const std::shared_ptr<arrow::Buffer> &source, std::shared_ptr<arrow::Buffer>* target) override;

 private:
  arrow::Status prepareFilter(const std::shared_ptr<arrow::Schema>& schema, std::shared_ptr<gandiva::Filter>& filter) const;

 private:
  std::vector<gandiva::ConditionPtr> conditions_;
};


