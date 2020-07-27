#include <sstream>

#include "graphite_parser.h"
#include "utils/string_utils.h"

GraphiteParser::GraphiteParser(const std::vector<std::string>& template_strings) {
  for (auto& template_string : template_strings) {
    templates_.emplace_back(template_string);
  }
}

arrow::Status GraphiteParser::parseRecordBatches(const std::shared_ptr<arrow::Buffer> &buffer,
                                                 std::vector<std::shared_ptr<arrow::RecordBatch>> &record_batches) {
  auto metric_strings = StringUtils::split(buffer->ToString(), "\n");
  parseMetricStrings(metric_strings);

  auto pool = arrow::default_memory_pool();
  KVContainer<arrow::StringBuilder> tags_id_to_builders;
  KVContainer<arrow::Type::type> fields_types;
  for (auto& metric : parsed_metrics_) {
    // Adding builders for tags
    for (auto& metric_tag : metric.second->tags_) {
      if (tags_id_to_builders.find(metric_tag.first) == tags_id_to_builders.end()) {
        tags_id_to_builders.emplace(metric_tag.first, pool);
      }
    }

    // Determining fields types for further usage
    for (auto& metric_field : metric.second->fields_) {
      auto metric_field_type = determineFieldType(metric_field.second);
      if (fields_types.find(metric_field.first) == fields_types.end()
          || (fields_types[metric_field.first] == arrow::Type::INT64 && metric_field_type != arrow::Type::INT64)
          || (fields_types[metric_field.first] != arrow::Type::STRING && metric_field_type == arrow::Type::STRING)) {
        fields_types[metric_field.first] = metric_field_type;
      }
    }
  }

  // Adding builders for fields depending on their types
  KVContainer<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
  for (auto& field : fields_types) {
    switch (field.second) {
      case arrow::Type::INT64:
        field_builders[field.first] = std::make_shared<arrow::Int64Builder>();
        break;
      case arrow::Type::DOUBLE:
        field_builders[field.first] = std::make_shared<arrow::DoubleBuilder>();
        break;
      case arrow::Type::STRING:
        field_builders[field.first] = std::make_shared<arrow::StringBuilder>();
        break;
      default:
        return arrow::Status::RError("Unexpected field type");
    }
  }

  char* str_end;
  arrow::StringBuilder measurement_name_builder;
  for (auto& metric : parsed_metrics_) {
    // Building measurement field
    ARROW_RETURN_NOT_OK(measurement_name_builder.Append(metric.second->measurement_name_));

    // Building tag fields
    for (auto& tag : tags_id_to_builders) {
      if (metric.second->tags_.find(tag.first) != metric.second->tags_.end()) {
        ARROW_RETURN_NOT_OK(tag.second.Append(metric.second->tags_[tag.first]));
      } else {
        ARROW_RETURN_NOT_OK(tag.second.AppendNull());
      }
    }

    // Building field fields
    for (auto& field : field_builders) {
      if (metric.second->fields_.find(field.first) != metric.second->fields_.end()) {
        auto field_value = metric.second->fields_[field.first];
        switch (fields_types[field.first]) {
          case arrow::Type::INT64:
            ARROW_RETURN_NOT_OK(std::static_pointer_cast<arrow::Int64Builder>(field.second)->Append(std::strtoll(field_value.data(), &str_end, 10)));
            break;
          case arrow::Type::DOUBLE:
            ARROW_RETURN_NOT_OK(std::static_pointer_cast<arrow::DoubleBuilder>(field.second)->Append(std::strtod(field_value.data(), &str_end)));
            break;
          case arrow::Type::STRING:
            ARROW_RETURN_NOT_OK(std::static_pointer_cast<arrow::StringBuilder>(field.second)->Append(field_value));
            break;
          default:
            return arrow::Status::RError("Unexpected field type");
        }
      } else {
        ARROW_RETURN_NOT_OK(field.second->AppendNull());
      }
    }
  }

  arrow::FieldVector fields;
  arrow::ArrayVector column_arrays;

  // Creating schema and finishing builders
  fields.push_back(arrow::field("measurement", arrow::utf8()));
  column_arrays.emplace_back();
  ARROW_RETURN_NOT_OK(measurement_name_builder.Finish(&column_arrays.back()));

  for (auto& tag : tags_id_to_builders) {
    fields.push_back(arrow::field(tag.first, arrow::utf8()));
    column_arrays.emplace_back();
    ARROW_RETURN_NOT_OK(tag.second.Finish(&column_arrays.back()));
  }

  for (auto& field : field_builders) {
    column_arrays.emplace_back();
    ARROW_RETURN_NOT_OK(field.second->Finish(&column_arrays.back()));
    switch (fields_types[field.first]) {
      case arrow::Type::INT64:
        fields.push_back(arrow::field(field.first, arrow::int64()));
        break;
      case arrow::Type::DOUBLE:
        fields.push_back(arrow::field(field.first, arrow::float64()));
        break;
      case arrow::Type::STRING:
        fields.push_back(arrow::field(field.first, arrow::utf8()));
        break;
      default:
        return arrow::Status::RError("Unexpected field type");
    }
  }

  record_batches.push_back(arrow::RecordBatch::Make(arrow::schema(fields),
      parsed_metrics_.size(),
      column_arrays));
  return arrow::Status::OK();
}

arrow::Type::type GraphiteParser::determineFieldType(const std::string& value) const {
  if (value == "0") {
    return arrow::Type::INT64;
  }

  char* str_end;
  auto int64_value = std::strtoll(value.data(), &str_end, 10);
  if (errno != ERANGE && int64_value != 0) {
    return arrow::Type::INT64;
  }

  auto double_value = std::strtod(value.data(), &str_end);
  if (errno != ERANGE && double_value != 0) {
    return arrow::Type::DOUBLE;
  }

  return arrow::Type::STRING;
}

void GraphiteParser::parseMetricStrings(const std::vector<std::string> &metric_strings) {
  for (auto& metric_string : metric_strings) {
    std::shared_ptr<Metric> metric{nullptr};
    for (auto& metric_template : templates_) {
      if ((metric = metric_template.buildMetric(metric_string)) != nullptr) {
        break;
      }
    }

    if (metric != nullptr) {
      auto metric_key_string = metric->getKeyString();
      auto parsed_metric_iter = parsed_metrics_.find(metric_key_string);
      if (parsed_metric_iter != parsed_metrics_.end()) {
        parsed_metric_iter->second->mergeWith(*metric);
      } else {
        parsed_metrics_[metric_key_string] = metric;
      }
    }
  }
}

GraphiteParser::Metric::Metric(std::string &&measurement_name, GraphiteParser::SortedKVContainer<std::string> &&tags)
    : measurement_name_(std::forward<std::string>(measurement_name))
    , tags_(std::forward<GraphiteParser::SortedKVContainer<std::string>>(tags)) {

}

std::string GraphiteParser::Metric::getKeyString() const {
  std::stringstream string_builder;
  string_builder << measurement_name_ << '.';
  for (auto& tag : tags_) {
    string_builder << tag.first << '=' << tag.second << '.';
  }

  return string_builder.str();
}

void GraphiteParser::Metric::mergeWith(const Metric& other) {
  if (getKeyString() != other.getKeyString()) {
    return;
  }

  for (auto& field : other.fields_) {
    if (fields_.find(field.first) != fields_.end()) {
      fields_[field.first] = field.second;
    }
  }
}

GraphiteParser::MetricTemplate::MetricTemplate(const std::string &template_string) {
  auto template_string_parts = StringUtils::split(template_string, " ");
  size_t part_idx = 0;

  if (template_string_parts.size() == 3 ||
      (template_string_parts.size() == 2 && template_string_parts[1].find('=') == std::string::npos)) {
    auto filter_regex_string = prepareFilterRegex(template_string_parts[0]);
    filter_ = std::make_shared<std::regex>(filter_regex_string);
    ++part_idx;
  }

  prepareTemplateParts(template_string_parts[part_idx]);
  ++part_idx;

  if (part_idx < template_string_parts.size()) {
    prepareAdditionalTags(template_string_parts[part_idx]);
  }
}

std::string GraphiteParser::MetricTemplate::prepareFilterRegex(const std::string &filter_string) const {
  std::stringstream regex_string_builder;
  for (auto& c : filter_string) {
    switch (c) {
      case '.':
        regex_string_builder << "\\.";
        break;
      case '*':
        regex_string_builder << "[\\w\\d]+";
        break;
      default:
        regex_string_builder << c;
    }
  }

  return regex_string_builder.str();
}

const std::string GraphiteParser::MetricTemplate::MEASUREMENT_PART_ID{"measurement"};
const std::string GraphiteParser::MetricTemplate::FIELD_PART_ID{"field"};
const std::string GraphiteParser::MetricTemplate::SEPARATOR{"_"};

void GraphiteParser::MetricTemplate::prepareTemplateParts(const std::string &template_string) {
  auto template_string_parts = StringUtils::split(template_string, ".");
  for (auto& part : template_string_parts) {
    if (part == MEASUREMENT_PART_ID) {
      parts_.push_back({TemplatePartType::MEASUREMENT});
    } else if (part == FIELD_PART_ID) {
      parts_.push_back({TemplatePartType::FIELD});
    } else {
      parts_.push_back({TemplatePartType::TAG, part});
    }
  }
}

void GraphiteParser::MetricTemplate::prepareAdditionalTags(const std::string &additional_tags_string) {
  auto additional_tags_parts = StringUtils::split(additional_tags_string, ",");
  for (auto& part : additional_tags_parts) {
    auto tag = StringUtils::split(part, "=");
    additional_tags_[tag[0]] = tag[1];
  }
}

bool GraphiteParser::MetricTemplate::match(const std::string &metric_string) const {
  auto metric_description_with_value = StringUtils::split(metric_string, " ");

  if (metric_description_with_value.size() < 2 ||
      (filter_ != nullptr && !std::regex_match(metric_description_with_value[0], *filter_))) {
    return false;
  }

  auto metric_description_parts = StringUtils::split(metric_description_with_value[0], ".");
  return metric_description_parts.size() == parts_.size();
}

std::shared_ptr<GraphiteParser::Metric> GraphiteParser::MetricTemplate::buildMetric(const std::string &metric_string) const {
  if (!match(metric_string)) {
    return nullptr;
  }

  auto metric_description_with_value = StringUtils::split(metric_string, " ");
  auto metric_description_parts = StringUtils::split(metric_description_with_value[0], ".");
  std::vector<std::string> measurement_name_parts;
  std::unordered_map<std::string, std::vector<std::string>> tags_parts;
  std::vector<std::string> field_parts;
  for (size_t i = 0; i < parts_.size(); ++i) {
    switch (parts_[i].type) {
      case MEASUREMENT:
        measurement_name_parts.push_back(metric_description_parts[i]);
        break;
      case FIELD:
        field_parts.push_back(metric_description_parts[i]);
        break;
      case TAG:
        if (tags_parts.find(parts_[i].id) == tags_parts.end()) {
          tags_parts.emplace(parts_[i].id, std::vector<std::string>{});
        }

        tags_parts[parts_[i].id].push_back(metric_description_parts[i]);
        break;
    }
  }

  GraphiteParser::SortedKVContainer<std::string> tags;
  for (auto& tag : tags_parts) {
    tags.emplace(tag.first, std::move(StringUtils::concatenateStrings(tag.second, SEPARATOR)));
  }

  for (auto& tag : additional_tags_) {
    tags.emplace(tag.first, tag.second);
  }

  auto metric = std::make_shared<Metric>(
      std::move(StringUtils::concatenateStrings(measurement_name_parts, SEPARATOR)),
      std::move(tags)
      );
  metric->fields_[StringUtils::concatenateStrings(field_parts, SEPARATOR)] = metric_description_with_value[1];
  return metric;
}
