#pragma once

#include <memory>

#include <gandiva/condition.h>
#include <gandiva/filter.h>

#include "data_handler.h"

class FilterHandler : public DataHandler {
 public:
  FilterHandler(std::shared_ptr<arrow::Schema>  schema, const std::vector<gandiva::ConditionPtr> & conditions);

  arrow::Status handle(std::shared_ptr<arrow::Buffer> source, std::shared_ptr<arrow::Buffer>* target) override;

 private:
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<gandiva::Filter> filter_;
};


