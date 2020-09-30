#include <map>

#include "data_converter.h"

arrow::Status DataConverter::convertTableToRecordBatch(const std::shared_ptr<arrow::Table>& table,
                                        std::shared_ptr<arrow::RecordBatch>* record_batch) {
  auto prepared_table_result = table->CombineChunks();
  if (!prepared_table_result.ok()) {
    return prepared_table_result.status();
  }

  arrow::ArrayVector table_columns;
  if (table->num_rows() != 0) {
    for (auto &column : prepared_table_result.ValueOrDie()->columns()) {
      table_columns.push_back(column->chunk(0));
    }
  }

  *record_batch = arrow::RecordBatch::Make(prepared_table_result.ValueOrDie()->schema(),
                                           prepared_table_result.ValueOrDie()->num_rows(),
                                           table_columns);
  return arrow::Status::OK();
}

arrow::Status DataConverter::concatenateRecordBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches,
                                              std::shared_ptr<arrow::RecordBatch>* target) {
  auto table_result = arrow::Table::FromRecordBatches(record_batches);
  if (!table_result.ok()) {
    return table_result.status();
  }

  ARROW_RETURN_NOT_OK(convertTableToRecordBatch(table_result.ValueOrDie(), target));
  return arrow::Status::OK();
}


arrow::Status DataConverter::convertToRecordBatches(const agent::PointBatch &points,
                                                    arrow::RecordBatchVector* record_batches,
                                                    const PointsToRecordBatchesConversionOptions& options) {
  auto pool = arrow::default_memory_pool();
  auto timestamp_builder = arrow::TimestampBuilder(arrow::timestamp(arrow::TimeUnit::SECOND), pool);
  auto measurement_builder = arrow::StringBuilder(pool);
  std::map<std::string, arrow::StringBuilder> tags_builders;
  std::map<std::string, arrow::DoubleBuilder> double_fields_builders;
  std::map<std::string, arrow::Int64Builder> int_fields_builders;
  std::map<std::string, arrow::StringBuilder> string_fields_builders;
  std::map<std::string, arrow::BooleanBuilder> bool_fields_builders;
  for (auto& point : points.points()) {
    addBuilders(point.tags(), &tags_builders, pool);
    addBuilders(point.fieldsint(), &int_fields_builders, pool);
    addBuilders(point.fieldsdouble(), &double_fields_builders, pool);
    addBuilders(point.fieldsstring(), &string_fields_builders, pool);
    addBuilders(point.fieldsbool(), &bool_fields_builders, pool);
  }

  for (auto& point : points.points()) {
    ARROW_RETURN_NOT_OK(timestamp_builder.Append(point.time()));
    ARROW_RETURN_NOT_OK(measurement_builder.Append(point.name()));
    ARROW_RETURN_NOT_OK(appendValues(point.tags(), &tags_builders));
    ARROW_RETURN_NOT_OK(appendValues(point.fieldsint(), &int_fields_builders));
    ARROW_RETURN_NOT_OK(appendValues(point.fieldsdouble(), &double_fields_builders));
    ARROW_RETURN_NOT_OK(appendValues(point.fieldsstring(), &string_fields_builders));
    ARROW_RETURN_NOT_OK(appendValues(point.fieldsbool(), &bool_fields_builders));
  }

  arrow::FieldVector schema_fields;
  arrow::ArrayVector column_arrays;

  schema_fields.push_back(arrow::field(options.timestamp_column_name, arrow::timestamp(arrow::TimeUnit::SECOND)));
  column_arrays.emplace_back();
  ARROW_RETURN_NOT_OK(timestamp_builder.Finish(&column_arrays.back()));

  schema_fields.push_back(arrow::field(options.measurement_column_name, arrow::utf8()));
  column_arrays.emplace_back();
  ARROW_RETURN_NOT_OK(measurement_builder.Finish(&column_arrays.back()));

  ARROW_RETURN_NOT_OK(buildColumnArrays(&column_arrays, &schema_fields, &tags_builders, arrow::utf8()));
  ARROW_RETURN_NOT_OK(buildColumnArrays(&column_arrays, &schema_fields, &int_fields_builders, arrow::int64()));
  ARROW_RETURN_NOT_OK(buildColumnArrays(&column_arrays, &schema_fields, &double_fields_builders, arrow::float64()));
  ARROW_RETURN_NOT_OK(buildColumnArrays(&column_arrays, &schema_fields, &string_fields_builders, arrow::utf8()));
  ARROW_RETURN_NOT_OK(buildColumnArrays(&column_arrays, &schema_fields, &bool_fields_builders, arrow::boolean()));

  record_batches->push_back(arrow::RecordBatch::Make(arrow::schema(schema_fields),
                                                    points.points_size(),
                                                    column_arrays));
  return arrow::Status::OK();
}

arrow::Status DataConverter::convertToPoints(const arrow::RecordBatchVector &record_batches,
                                             agent::PointBatch* points,
                                             const RecordBatchesToPointsConversionOptions& options) {
  size_t points_count = 0;
  for (auto& record_batch : record_batches) {
    points->mutable_points()->Reserve(points_count + record_batch->num_rows());
    for (int i = 0; i < record_batch->num_columns(); ++i) {
      auto& column_name = record_batch->column_name(i);
      auto column = record_batch->column(i);
      for (size_t j = 0; j < column->length(); ++j) {
        auto get_scalar_result = column->GetScalar(j);
        if (!get_scalar_result.ok()) {
          return get_scalar_result.status();
        }

        if (i == 0) {
          points->mutable_points()->Add();
        }

        auto& point = points->mutable_points()->operator[](points_count + j);
        auto scalar_value = get_scalar_result.ValueOrDie();
        if (column_name == options.measurement_column_name) {
          point.set_name(scalar_value->ToString());
        } else if (column_name == options.timestamp_column_name) {
          point.set_time(std::static_pointer_cast<arrow::Int64Scalar>(scalar_value)->value);
        } else {
          switch (column->type_id()) {
            case arrow::Type::INT64:
              point.mutable_fieldsint()->operator[](column_name) = std::static_pointer_cast<arrow::Int64Scalar>(scalar_value)->value;
              break;
            case arrow::Type::DOUBLE:
              point.mutable_fieldsdouble()->operator[](column_name) = std::static_pointer_cast<arrow::DoubleScalar>(scalar_value)->value;
              break;
            case arrow::Type::STRING:
              if (options.tag_columns_names.find(column_name) != options.tag_columns_names.end()) {
                point.mutable_tags()->operator[](column_name) = scalar_value->ToString();
              } else {
                point.mutable_fieldsstring()->operator[](column_name) = scalar_value->ToString();
              }

              break;
            case arrow::Type::BOOL:
              point.mutable_fieldsbool()->operator[](column_name) = std::static_pointer_cast<arrow::BooleanScalar>(scalar_value)->value;
              break;
            default:
              return arrow::Status::NotImplemented("Currently supports field columns of arrow types:"
                                                   "{arrow::int64, arrow::float64, arrow::utf8, arrow::boolean}");
          }
        }
      }
    }

    points_count += record_batch->num_rows();
  }

  return arrow::Status::OK();
}

template<typename T, typename BuilderType>
void DataConverter::addBuilders(const google::protobuf::Map<std::string, T> &data_map,
                                std::map<std::string, BuilderType>* builders,
                                arrow::MemoryPool* pool) {
  for (auto& value : data_map) {
    if (builders->find(value.first) == builders->end()) {
      builders->emplace(value.first, pool);
    }
  }
}

template<typename T, typename BuilderType>
arrow::Status DataConverter::appendValues(const google::protobuf::Map<std::string, T> &data_map,
                                          std::map<std::string, BuilderType>* builders) {
  for (auto& [field_name, builder] : *builders) {
    if (data_map.find(field_name) != data_map.end()) {
      ARROW_RETURN_NOT_OK(builder.Append(data_map.at(field_name)));
    } else {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    }
  }

  return arrow::Status::OK();
}

template<typename BuilderType>
arrow::Status DataConverter::buildColumnArrays(arrow::ArrayVector* column_arrays,
                                               arrow::FieldVector* schema_fields,
                                               std::map<std::string, BuilderType>* builders,
                                               const std::shared_ptr<arrow::DataType> &data_type) {
  for (auto& [field_name, builder] : *builders) {
    schema_fields->push_back(arrow::field(field_name, data_type));
    column_arrays->emplace_back();
    ARROW_RETURN_NOT_OK(builder.Finish(&column_arrays->back()));
  }

  return arrow::Status::OK();
}
