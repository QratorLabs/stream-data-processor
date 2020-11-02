#include <unordered_map>

#include <spdlog/spdlog.h>

#include "metadata/column_typing.h"
#include "metadata/grouping.h"
#include "points_converter.h"
#include "group_parser.h"

arrow::Status PointsConverter::convertToRecordBatches(
    const agent::PointBatch& points, arrow::RecordBatchVector* record_batches,
    const PointsToRecordBatchesConversionOptions& options) {
  std::unordered_map<std::string, std::vector<size_t>> groups_indexes;
  for (size_t i = 0; i < points.points_size(); ++i) {
    auto point_group_string = points.points(i).group();
    if (groups_indexes.find(point_group_string) == groups_indexes.end()) {
      groups_indexes[point_group_string] = std::vector{ i };
    } else {
      groups_indexes[point_group_string].push_back(i);
    }
  }

  for (auto& [group_string, group_indexes] : groups_indexes) {
    record_batches->emplace_back();
    ARROW_RETURN_NOT_OK(convertPointsGroup(
        points, group_string, group_indexes,
        &record_batches->back(), options));
  }

  return arrow::Status::OK();
}

arrow::Status PointsConverter::convertToPoints(
    const arrow::RecordBatchVector& record_batches, agent::PointBatch* points) {
  int points_count = 0;
  for (auto& record_batch : record_batches) {
    points->mutable_points()->Reserve(points_count +
        record_batch->num_rows());

    std::string time_column_name;
    ARROW_RETURN_NOT_OK(ColumnTyping::getTimeColumnNameMetadata(
        record_batch, &time_column_name));

    std::string measurement_column_name;
    ARROW_RETURN_NOT_OK(ColumnTyping::getMeasurementColumnNameMetadata(
        record_batch, &measurement_column_name));

    auto group = RecordBatchGrouping::extractGroup(record_batch);

    auto group_string = GroupParser::encode(
        group, measurement_column_name);

    for (int i = 0; i < record_batch->num_columns(); ++i) {
      auto& column_name = record_batch->column_name(i);
      auto column = record_batch->column(i);
      auto column_type = ColumnTyping::getColumnType(
          record_batch->schema()->field(i));

      for (int j = 0; j < column->length(); ++j) {
        auto get_scalar_result = column->GetScalar(j);
        if (!get_scalar_result.ok()) {
          return get_scalar_result.status();
        }

        if (i == 0) {
          points->mutable_points()->Add();
          points->mutable_points()->Mutable(
              points_count + j)->set_group(group_string);
        }

        auto& point = points->mutable_points()->operator[](points_count + j);
        auto scalar_value = get_scalar_result.ValueOrDie();

        bool skip_column = false;

        switch (column_type) {
          case MEASUREMENT:
            point.set_name(scalar_value->ToString());
            break;
          case TIME:
            point.set_time(
                std::static_pointer_cast<arrow::Int64Scalar>(scalar_value)
                    ->value);
            break;
          case TAG:
            point.mutable_tags()->operator[](column_name) =
                scalar_value->ToString();
            break;
          case FIELD:
            switch (column->type_id()) {
              case arrow::Type::INT64:
                point.mutable_fieldsint()->operator[](column_name) =
                    std::static_pointer_cast<arrow::Int64Scalar>(scalar_value)
                        ->value;
                break;
              case arrow::Type::DOUBLE:
                point.mutable_fieldsdouble()->operator[](column_name) =
                    std::static_pointer_cast<arrow::DoubleScalar>(scalar_value)
                        ->value;
                break;
              case arrow::Type::STRING:
                point.mutable_fieldsstring()->operator[](column_name) =
                    scalar_value->ToString();
                break;
              case arrow::Type::BOOL:
                point.mutable_fieldsbool()->operator[](column_name) =
                    std::static_pointer_cast<arrow::BooleanScalar>(
                        scalar_value)
                        ->value;
                break;
              default:
                spdlog::warn(
                    "Currently supports field columns of arrow types: "
                    "arrow::int64, arrow::float64, arrow::utf8, "
                    "arrow::boolean. "
                    "Provided type is {}, column name: {}",
                    column->type()->ToString(),
                    record_batch->schema()->field(i)->name());
                skip_column = true;
            }

            break;
          default:
            return arrow::Status::Invalid(fmt::format(
                "Can't convert RecordBatch to points. "
                "Found column with UNKNOWN type: "
                "{}", record_batch->schema()->field(i)->name()
            ));
        }

        if (skip_column) {
          break;
        }
      }
    }

    points_count += record_batch->num_rows();
  }

  return arrow::Status::OK();
}

template <typename T, typename BuilderType>
void PointsConverter::addBuilders(
    const google::protobuf::Map<std::string, T>& data_map,
    std::map<std::string, BuilderType>* builders, arrow::MemoryPool* pool) {
  for (auto& value : data_map) {
    if (builders->find(value.first) == builders->end()) {
      builders->emplace(value.first, pool);
    }
  }
}

template <typename T, typename BuilderType>
arrow::Status PointsConverter::appendValues(
    const google::protobuf::Map<std::string, T>& data_map,
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

template <typename BuilderType>
arrow::Status PointsConverter::buildColumnArrays(
    arrow::ArrayVector* column_arrays, arrow::FieldVector* schema_fields,
    std::map<std::string, BuilderType>* builders,
    const std::shared_ptr<arrow::DataType>& data_type,
    ColumnType column_type) {
  for (auto& [field_name, builder] : *builders) {
    schema_fields->push_back(arrow::field(field_name, data_type));
    ARROW_RETURN_NOT_OK(ColumnTyping::setColumnTypeMetadata(
        &schema_fields->back(), column_type));

    column_arrays->emplace_back();
    ARROW_RETURN_NOT_OK(builder.Finish(&column_arrays->back()));
  }

  return arrow::Status::OK();
}
arrow::Status PointsConverter::convertPointsGroup(
    const agent::PointBatch& points,
    const std::string& group_string,
    const std::vector<size_t>& group_indexes,
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const PointsToRecordBatchesConversionOptions& options) {
  auto pool = arrow::default_memory_pool();
  auto timestamp_builder = arrow::TimestampBuilder(
      arrow::timestamp(arrow::TimeUnit::MILLI), pool);
  auto measurement_builder = arrow::StringBuilder(pool);
  std::map<std::string, arrow::StringBuilder> tags_builders;
  std::map<std::string, arrow::DoubleBuilder> double_fields_builders;
  std::map<std::string, arrow::Int64Builder> int_fields_builders;
  std::map<std::string, arrow::StringBuilder> string_fields_builders;
  std::map<std::string, arrow::BooleanBuilder> bool_fields_builders;
  for (auto& i : group_indexes) {
    auto& point = points.points(i);
    addBuilders(point.tags(), &tags_builders, pool);
    addBuilders(point.fieldsint(), &int_fields_builders, pool);
    addBuilders(point.fieldsdouble(), &double_fields_builders, pool);
    addBuilders(point.fieldsstring(), &string_fields_builders, pool);
    addBuilders(point.fieldsbool(), &bool_fields_builders, pool);
  }

  for (auto& i : group_indexes) {
    auto& point = points.points(i);
    ARROW_RETURN_NOT_OK(timestamp_builder.Append(point.time()));
    ARROW_RETURN_NOT_OK(measurement_builder.Append(point.name()));
    ARROW_RETURN_NOT_OK(appendValues(point.tags(), &tags_builders));
    ARROW_RETURN_NOT_OK(
        appendValues(point.fieldsint(), &int_fields_builders));
    ARROW_RETURN_NOT_OK(
        appendValues(point.fieldsdouble(), &double_fields_builders));
    ARROW_RETURN_NOT_OK(
        appendValues(point.fieldsstring(), &string_fields_builders));
    ARROW_RETURN_NOT_OK(
        appendValues(point.fieldsbool(), &bool_fields_builders));
  }

  arrow::FieldVector schema_fields;
  arrow::ArrayVector column_arrays;

  schema_fields.push_back(arrow::field(
      options.time_column_name,
      arrow::timestamp(arrow::TimeUnit::MILLI)));
  ARROW_RETURN_NOT_OK(ColumnTyping::setColumnTypeMetadata(
      &schema_fields.back(), TIME));
  column_arrays.emplace_back();
  ARROW_RETURN_NOT_OK(timestamp_builder.Finish(&column_arrays.back()));

  schema_fields.push_back(
      arrow::field(options.measurement_column_name, arrow::utf8()));
  ARROW_RETURN_NOT_OK(ColumnTyping::setColumnTypeMetadata(
      &schema_fields.back(), MEASUREMENT));
  column_arrays.emplace_back();
  ARROW_RETURN_NOT_OK(measurement_builder.Finish(&column_arrays.back()));

  ARROW_RETURN_NOT_OK(buildColumnArrays(&column_arrays, &schema_fields,
                                        &tags_builders, arrow::utf8(), TAG));
  ARROW_RETURN_NOT_OK(buildColumnArrays(&column_arrays, &schema_fields,
                                        &int_fields_builders,
                                        arrow::int64(), FIELD));
  ARROW_RETURN_NOT_OK(buildColumnArrays(&column_arrays, &schema_fields,
                                        &double_fields_builders,
                                        arrow::float64(), FIELD));
  ARROW_RETURN_NOT_OK(buildColumnArrays(&column_arrays, &schema_fields,
                                        &string_fields_builders,
                                        arrow::utf8(), FIELD));
  ARROW_RETURN_NOT_OK(buildColumnArrays(&column_arrays, &schema_fields,
                                        &bool_fields_builders,
                                        arrow::boolean(), FIELD));

  *record_batch = arrow::RecordBatch::Make(
      arrow::schema(schema_fields), group_indexes.size(), column_arrays);

  RecordBatchGroup group;
  try {
    group = GroupParser::parse(group_string, options.measurement_column_name);
  } catch (const GroupParserException& exc) {
    return arrow::Status::Invalid(fmt::format(
        "Error while parsing group string: {}", group_string));
  }

  ARROW_RETURN_NOT_OK(RecordBatchGrouping::setGroupMetadata(
      record_batch, group));

  ARROW_RETURN_NOT_OK(ColumnTyping::setTimeColumnNameMetadata(
      record_batch, options.time_column_name));
  ARROW_RETURN_NOT_OK(ColumnTyping::setMeasurementColumnNameMetadata(
      record_batch, options.measurement_column_name));

  return arrow::Status::OK();
}
