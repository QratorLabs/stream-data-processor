#include <set>
#include <sstream>
#include <unordered_map>

#include "join_handler.h"
#include "utils/utils.h"

arrow::Status JoinHandler::handle(
    const arrow::RecordBatchVector& record_batches,
    arrow::RecordBatchVector* result) {
  auto pool = arrow::default_memory_pool();

  std::unordered_map<std::string, std::shared_ptr<arrow::DataType>>
      column_types;

  std::unordered_map<std::string, std::shared_ptr<arrow::ArrayBuilder>>
      column_builders;

  std::unordered_map<std::string, std::set<JoinValue, JoinValueCompare>>
      keys_to_rows;

  JoinKey join_key;
  for (size_t i = 0; i < record_batches.size(); ++i) {
    for (auto& field : record_batches[i]->schema()->fields()) {
      if (column_builders.find(field->name()) == column_builders.end()) {
        column_builders[field->name()] =
            std::shared_ptr<arrow::ArrayBuilder>();

        ARROW_RETURN_NOT_OK(ArrowUtils::makeArrayBuilder(
            field->type()->id(), &column_builders[field->name()], pool));

        column_types[field->name()] = field->type();
      }
    }

    for (size_t j = 0; j < record_batches[i]->num_rows(); ++j) {
      ARROW_RETURN_NOT_OK(getJoinKey(record_batches[i], j, &join_key));
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

        auto get_scalar_result =
            record_batch->column(i)->GetScalar(value.row_idx);
        if (!get_scalar_result.ok()) {
          return get_scalar_result.status();
        }

        ARROW_RETURN_NOT_OK(ArrowUtils::appendToBuilder(
            get_scalar_result.ValueOrDie(), &column_builders[column_name],
            record_batch->schema()->field(i)->type()->id()));

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

  ARROW_RETURN_NOT_OK(ComputeUtils::sortByColumn(
      time_column_name_, result_record_batch, &result_record_batch));

  result->push_back(result_record_batch);
  return arrow::Status::OK();
}

arrow::Status JoinHandler::getJoinKey(
    const std::shared_ptr<arrow::RecordBatch>& record_batch, size_t row_idx,
    JoinHandler::JoinKey* join_key) const {
  std::stringstream key_string_builder;
  for (auto& join_column_name : join_on_columns_) {
    auto get_scalar_result =
        record_batch->GetColumnByName(join_column_name)->GetScalar(row_idx);

    if (!get_scalar_result.ok()) {
      return get_scalar_result.status();
    }

    key_string_builder << join_column_name << '='
                       << get_scalar_result.ValueOrDie()->ToString() << ',';
  }

  join_key->key_string = std::move(key_string_builder.str());

  auto get_scalar_result =
      record_batch->GetColumnByName(time_column_name_)->GetScalar(row_idx);

  if (!get_scalar_result.ok()) {
    return get_scalar_result.status();
  }

  join_key->time = std::static_pointer_cast<arrow::Int64Scalar>(
                       get_scalar_result.ValueOrDie())
                       ->value;

  return arrow::Status::OK();
}

arrow::Status JoinHandler::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
    arrow::RecordBatchVector* result) {
  ARROW_RETURN_NOT_OK(handle({record_batch}, result));
  return arrow::Status::OK();
}
