#include <set>
#include <sstream>
#include <unordered_map>

#include "join_handler.h"
#include "metadata/metadata.h"
#include "utils/utils.h"

namespace stream_data_processor {

arrow::Result<arrow::RecordBatchVector> JoinHandler::handle(
    const arrow::RecordBatchVector& record_batches) {
  if (record_batches.empty()) {
    return arrow::RecordBatchVector{};
  }

  auto pool = arrow::default_memory_pool();

  std::unordered_map<std::string, std::shared_ptr<arrow::DataType>>
      column_types;

  std::unordered_map<
      std::string,
      std::shared_ptr<arrow::ArrayBuilder>>  // TODO: check fields on quality
      column_builders;

  std::unordered_map<std::string, std::set<JoinValue, JoinValueCompare>>
      keys_to_rows;

  ARROW_ASSIGN_OR_RAISE(
      auto time_column_name,
      metadata::getTimeColumnNameMetadata(*record_batches.front()));

  for (size_t i = 0; i < record_batches.size(); ++i) {
    for (auto& field : record_batches[i]->schema()->fields()) {
      if (column_builders.find(field->name()) == column_builders.end()) {
        column_builders[field->name()] =
            std::shared_ptr<arrow::ArrayBuilder>();

        if (field->type()->id() == arrow::Type::TIMESTAMP) {
          ARROW_ASSIGN_OR_RAISE(
              column_builders[field->name()],
              arrow_utils::createTimestampArrayBuilder(field->type(), pool));
        } else {
          ARROW_ASSIGN_OR_RAISE(
              column_builders[field->name()],
              arrow_utils::createArrayBuilder(field->type()->id(), pool));
        }

        column_types[field->name()] = field->type();
      }
    }

    for (size_t j = 0; j < record_batches[i]->num_rows(); ++j) {
      JoinKey join_key;
      ARROW_ASSIGN_OR_RAISE(
          join_key, getJoinKey(record_batches[i], j, time_column_name));

      if (keys_to_rows.find(join_key.key_string) == keys_to_rows.end()) {
        keys_to_rows[join_key.key_string] =
            std::set<JoinValue, JoinValueCompare>();
      }

      keys_to_rows[join_key.key_string].insert({i, j, join_key.time});
    }
  }

  std::unordered_map<std::string, bool> filled;
  for (auto& column : column_builders) { filled[column.first] = false; }

  size_t row_count = 0;
  for (auto& joined_values : keys_to_rows) {
    if (joined_values.second.empty()) {
      continue;
    }

    auto last_ts = joined_values.second.begin()->time;
    for (auto& column : column_builders) { filled[column.first] = false; }

    for (auto& value : joined_values.second) {
      if (std::abs(value.time - last_ts) > tolerance_) {
        for (auto& column : filled) {
          if (!column.second) {
            ARROW_RETURN_NOT_OK(column_builders[column.first]->AppendNull());
          } else {
            column.second = false;
          }
        }

        last_ts = value.time;
        ++row_count;
      }

      auto record_batch = record_batches[value.record_batch_idx];
      for (size_t i = 0; i < record_batch->num_columns(); ++i) {
        auto column_name = record_batch->schema()->field_names()[i];
        if (filled[column_name]) {
          continue;
        }

        std::shared_ptr<arrow::Scalar> value_scalar;
        ARROW_ASSIGN_OR_RAISE(
            value_scalar, record_batch->column(i)->GetScalar(value.row_idx));

        if (record_batch->schema()->field(i)->type()->id() ==
            arrow::Type::TIMESTAMP) {
          ARROW_RETURN_NOT_OK(arrow_utils::appendToTimestampBuilder(
              value_scalar, &column_builders[column_name],
              record_batch->schema()->field(i)->type()));
        } else {
          ARROW_RETURN_NOT_OK(arrow_utils::appendToBuilder(
              value_scalar, &column_builders[column_name],
              record_batch->schema()->field(i)->type()->id()));
        }

        filled[column_name] = true;
      }
    }

    for (auto& column : filled) {
      if (!column.second) {
        ARROW_RETURN_NOT_OK(column_builders[column.first]->AppendNull());
      } else {
        column.second = false;
      }
    }

    ++row_count;
  }

  arrow::FieldVector fields;
  arrow::ArrayVector result_arrays;
  for (auto& column : column_builders) {
    result_arrays.emplace_back();
    ARROW_RETURN_NOT_OK(column.second->Finish(&result_arrays.back()));
    fields.push_back(arrow::field(column.first, column_types[column.first]));
  }

  auto result_record_batch = arrow::RecordBatch::Make(
      arrow::schema(fields), row_count, result_arrays);

  ARROW_ASSIGN_OR_RAISE(
      result_record_batch,
      compute_utils::sortByColumn(time_column_name, result_record_batch));

  ARROW_RETURN_NOT_OK(metadata::setTimeColumnNameMetadata(
      &result_record_batch,
      time_column_name));  // TODO: set measurement column name metadata

  return arrow::RecordBatchVector{
      result_record_batch};  // TODO: copy column types
}

arrow::Result<JoinHandler::JoinKey> JoinHandler::getJoinKey(
    const std::shared_ptr<arrow::RecordBatch>& record_batch, size_t row_idx,
    std::string time_column_name) const {
  std::stringstream key_string_builder;
  for (auto& join_column_name : join_on_columns_) {
    std::shared_ptr<arrow::Scalar> value_scalar;
    ARROW_ASSIGN_OR_RAISE(
        value_scalar,
        record_batch->GetColumnByName(join_column_name)->GetScalar(row_idx));

    key_string_builder << join_column_name << '=' << value_scalar->ToString()
                       << ',';
  }

  JoinKey join_key;
  join_key.key_string = std::move(key_string_builder.str());

  auto time_column = record_batch->GetColumnByName(time_column_name);
  if (time_column == nullptr) {
    return arrow::Status::Invalid(fmt::format(
        "Time column with name {} should be presented", time_column_name));
  }

  ARROW_ASSIGN_OR_RAISE(auto value_scalar, time_column->GetScalar(row_idx));

  if (value_scalar->type->id() == arrow::Type::TIMESTAMP) {
    ARROW_ASSIGN_OR_RAISE(
        value_scalar,
        value_scalar->CastTo(arrow::timestamp(arrow::TimeUnit::SECOND)));

    join_key.time =
        std::static_pointer_cast<arrow::TimestampScalar>(value_scalar)->value;
  } else {
    if (value_scalar->type->id() != arrow::Type::INT64) {
      return arrow::Status::NotImplemented(
          "JoinHandler currently supports arrow::Type::INT64 type as "
          "non-timestamp time column's type only");
    }

    ARROW_ASSIGN_OR_RAISE(
        auto value_time_unit,
        metadata::getTimeUnitMetadata(*record_batch, time_column_name));

    int64_t time_value =
        std::static_pointer_cast<arrow::Int64Scalar>(value_scalar)->value;

    ARROW_ASSIGN_OR_RAISE(join_key.time,
                          time_utils::convertTime(time_value, value_time_unit,
                                                  time_utils::SECOND));
  }

  return join_key;
}

arrow::Result<arrow::RecordBatchVector> JoinHandler::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  return handle(arrow::RecordBatchVector{record_batch});
}

}  // namespace stream_data_processor
