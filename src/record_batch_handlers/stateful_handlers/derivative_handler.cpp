#include <spdlog/spdlog.h>

#include "derivative_handler.h"
#include "metadata/metadata.h"
#include "utils/utils.h"

namespace stream_data_processor {

arrow::Result<arrow::RecordBatchVector> DerivativeHandler::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  ARROW_ASSIGN_OR_RAISE(auto time_column_name,
                        metadata::getTimeColumnNameMetadata(*record_batch));

  ARROW_ASSIGN_OR_RAISE(
      auto sorted_record_batch,
      compute_utils::sortByColumn(time_column_name, record_batch));

  auto concat_record_batch = sorted_record_batch;
  if (buffered_batch_ != nullptr) {
    ARROW_ASSIGN_OR_RAISE(concat_record_batch,
                          convert_utils::concatenateRecordBatches(
                              {buffered_batch_, sorted_record_batch}));
  }

  auto time_column = concat_record_batch->GetColumnByName(time_column_name);
  if (time_column == nullptr) {
    return arrow::Status::Invalid(fmt::format(
        "Concatenated RecordBatch has no time column with name {}",
        time_column_name));
  }

  std::unordered_map<std::string, arrow::Array*> value_columns;
  std::unordered_map<std::string, arrow::DoubleBuilder>
      derivative_columns_builders;
  for (const auto& [result_column_name, derivative_case] :
       options_.derivative_cases) {
    auto& value_column_name = derivative_case.values_column_name;
    if (value_columns.find(value_column_name) == value_columns.end()) {
      value_columns[value_column_name] =
          concat_record_batch->GetColumnByName(value_column_name).get();

      if (value_columns[value_column_name] == nullptr) {
        return arrow::Status::KeyError(fmt::format(
            "Concatenated RecordBatch has not column with name {} "
            "to calculate derivative",
            value_column_name));
      }
    }

    derivative_columns_builders[result_column_name];
  }

  double left_bound_time, derivative_time, right_bound_time;
  if (all_buffered_times_.empty()) {
    ARROW_ASSIGN_OR_RAISE(left_bound_time,
                          getScaledPositionTime(0, *time_column));
  } else {
    left_bound_time = all_buffered_times_.front();
  }

  int64_t derivative_row_id = 0;
  int64_t right_bound_row_id = 0;
  if (buffered_batch_ != nullptr) {
    right_bound_row_id = buffered_batch_->num_rows();
  }

  ARROW_ASSIGN_OR_RAISE(
      right_bound_time,
      getScaledPositionTime(right_bound_row_id, *time_column));

  auto total_rows = time_column->length();
  while (derivative_row_id < total_rows) {
    ARROW_ASSIGN_OR_RAISE(
        derivative_time,
        getScaledPositionTime(derivative_row_id, *time_column));

    while ((derivative_time - left_bound_time) *
               options_.unit_time_segment.count() >
           options_.derivative_neighbourhood.count()) {
      all_buffered_times_.pop_front();
      for ([[maybe_unused]] auto& [_, buffered_values] : buffered_values_) {
        while (!buffered_values.times.empty() &&
               (derivative_time - buffered_values.times.front()) *
                       options_.unit_time_segment.count() >
                   options_.derivative_neighbourhood.count()) {
          buffered_values.times.pop_front();
          buffered_values.values.pop_front();
        }
      }

      if (all_buffered_times_.empty()) {
        left_bound_time = derivative_time;
      } else {
        left_bound_time = all_buffered_times_.front();
      }
    }

    while (right_bound_row_id < total_rows &&
           (right_bound_time - derivative_time) *
                   options_.unit_time_segment.count() <=
               options_.derivative_neighbourhood.count()) {
      all_buffered_times_.push_back(right_bound_time);
      for (auto& [column_name, column] : value_columns) {
        ARROW_ASSIGN_OR_RAISE(auto value_scalar,
                              column->GetScalar(right_bound_row_id));

        if (!value_scalar->is_valid) {
          continue;
        }

        ARROW_ASSIGN_OR_RAISE(value_scalar,
                              value_scalar->CastTo(arrow::float64()));

        buffered_values_[column_name].times.push_back(right_bound_time);
        buffered_values_[column_name].values.push_back(
            std::static_pointer_cast<arrow::DoubleScalar>(value_scalar)
                ->value);
      }

      ++right_bound_row_id;
      if (right_bound_row_id >= total_rows) {
        break;
      }

      ARROW_ASSIGN_OR_RAISE(
          right_bound_time,
          getScaledPositionTime(right_bound_row_id, *time_column));
    }

    if (!options_.no_wait_future &&
        (right_bound_time - derivative_time) *
                options_.unit_time_segment.count() <=
            options_.derivative_neighbourhood.count()) {
      break;
    }

    for (const auto& [result_column_name, derivative_case] :
         options_.derivative_cases) {
      try {
        ARROW_RETURN_NOT_OK(
            derivative_columns_builders[result_column_name].Append(
                derivative_calculator_->calculateDerivative(
                    buffered_values_.at(derivative_case.values_column_name)
                        .times,
                    buffered_values_.at(derivative_case.values_column_name)
                        .values,
                    derivative_time, derivative_case.order)));
      } catch (const compute_utils::ComputeException& exc) {
        spdlog::warn(
            "ComputeException thrown while calculating {} order "
            "derivative on column {} with message: {}",
            derivative_case.order, derivative_case.values_column_name,
            exc.what());

        ARROW_RETURN_NOT_OK(
            derivative_columns_builders[result_column_name].AppendNull());
      }
    }

    ++derivative_row_id;
  }

  buffered_batch_ = concat_record_batch->Slice(derivative_row_id);
  auto calculated_batch = concat_record_batch->Slice(0, derivative_row_id);
  copySchemaMetadata(*record_batch, &calculated_batch);
  ARROW_RETURN_NOT_OK(copyColumnTypes(*record_batch, &calculated_batch));

  for (auto& [result_column_name, column_builder] :
       derivative_columns_builders) {
    ARROW_ASSIGN_OR_RAISE(auto result_column, column_builder.Finish());
    auto result_field = arrow::field(result_column_name, arrow::float64());

    ARROW_RETURN_NOT_OK(
        metadata::setColumnTypeMetadata(&result_field, metadata::FIELD));

    ARROW_ASSIGN_OR_RAISE(
        calculated_batch,
        calculated_batch->AddColumn(calculated_batch->num_columns(),
                                    result_field, result_column));
  }

  return arrow::RecordBatchVector{calculated_batch};
}

arrow::Result<double> DerivativeHandler::getScaledPositionTime(
    int64_t row_id, const arrow::Array& time_column) const {
  ARROW_ASSIGN_OR_RAISE(auto time_scalar, arrow_utils::castTimestampScalar(
                                              time_column.GetScalar(row_id),
                                              arrow::TimeUnit::NANO));

  return std::static_pointer_cast<arrow::TimestampScalar>(time_scalar)
             ->value /
         options_.unit_time_segment.count();
}

std::shared_ptr<RecordBatchHandler> DerivativeHandlerFactory::createHandler()
    const {
  return std::make_shared<DerivativeHandler>(derivative_calculator_,
                                             options_);
}

}  // namespace stream_data_processor
