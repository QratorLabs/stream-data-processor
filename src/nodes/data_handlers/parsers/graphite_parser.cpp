#include <ctime>
#include <sstream>

#include "graphite_parser.h"
#include "metadata/column_typing.h"
#include "utils/string_utils.h"

namespace stream_data_processor {

GraphiteParser::GraphiteParser(const GraphiteParserOptions& parser_options)
    : separator_(parser_options.separator),
      time_column_name_(parser_options.time_column_name),
      measurement_column_name_(parser_options.measurement_column_name) {
  for (auto& template_string : parser_options.template_strings) {
    templates_.emplace_back(template_string);
  }
}

arrow::Result<arrow::RecordBatchVector> GraphiteParser::parseRecordBatches(
    const arrow::Buffer& buffer) {
  auto metric_strings = string_utils::split(buffer.ToString(), "\n");
  if (metric_strings.empty()) {
    return arrow::RecordBatchVector{};
  }

  parseMetricStrings(metric_strings);

  auto pool = arrow::default_memory_pool();
  SortedKVContainer<arrow::StringBuilder> tags_id_to_builders;
  KVContainer<arrow::Type::type> fields_types;
  for (auto& metric : parsed_metrics_) {
    // Adding builders for tags
    for (auto& metric_tag : metric->tags) {
      if (tags_id_to_builders.find(metric_tag.first) ==
          tags_id_to_builders.end()) {
        tags_id_to_builders.emplace(metric_tag.first, pool);
      }
    }

    // Determining fields types for further usage
    for (auto& metric_field : metric->fields) {
      auto metric_field_type = determineFieldType(metric_field.second);
      if (fields_types.find(metric_field.first) == fields_types.end() ||
          (fields_types[metric_field.first] == arrow::Type::INT64 &&
           metric_field_type != arrow::Type::INT64) ||
          (fields_types[metric_field.first] != arrow::Type::STRING &&
           metric_field_type == arrow::Type::STRING)) {
        fields_types[metric_field.first] = metric_field_type;
      }
    }
  }

  // Adding builders for fields depending on their types
  SortedKVContainer<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
  for (auto& field : fields_types) {
    switch (field.second) {
      case arrow::Type::INT64:
        field_builders[field.first] = std::make_shared<arrow::Int64Builder>();
        break;
      case arrow::Type::DOUBLE:
        field_builders[field.first] =
            std::make_shared<arrow::DoubleBuilder>();
        break;
      case arrow::Type::STRING:
        field_builders[field.first] =
            std::make_shared<arrow::StringBuilder>();
        break;
      default: return arrow::Status::ExecutionError("Unexpected field type");
    }
  }

  char* str_end = nullptr;
  arrow::TimestampBuilder timestamp_builder(
      arrow::timestamp(arrow::TimeUnit::SECOND), pool);
  arrow::StringBuilder measurement_name_builder;
  for (auto& metric : parsed_metrics_) {
    // Building timestamp field
    if (metric->timestamp != -1) {
      ARROW_RETURN_NOT_OK(timestamp_builder.Append(metric->timestamp));
    } else {
      ARROW_RETURN_NOT_OK(timestamp_builder.Append(std::time(nullptr)));
    }

    // Building measurement field
    ARROW_RETURN_NOT_OK(
        measurement_name_builder.Append(metric->measurement_name));

    // Building tag fields
    for (auto& tag : tags_id_to_builders) {
      if (metric->tags.find(tag.first) != metric->tags.end()) {
        ARROW_RETURN_NOT_OK(tag.second.Append(metric->tags[tag.first]));
      } else {
        ARROW_RETURN_NOT_OK(tag.second.AppendNull());
      }
    }

    // Building field fields
    for (auto& field : field_builders) {
      if (metric->fields.find(field.first) != metric->fields.end()) {
        auto field_value = metric->fields[field.first];
        switch (fields_types[field.first]) {
          case arrow::Type::INT64:
            ARROW_RETURN_NOT_OK(
                std::static_pointer_cast<arrow::Int64Builder>(field.second)
                    ->Append(std::strtoll(field_value.c_str(), &str_end,
                                          NUMBER_PARSING_BASE)));
            break;
          case arrow::Type::DOUBLE:
            ARROW_RETURN_NOT_OK(
                std::static_pointer_cast<arrow::DoubleBuilder>(field.second)
                    ->Append(std::strtod(field_value.c_str(), &str_end)));
            break;
          case arrow::Type::STRING:
            ARROW_RETURN_NOT_OK(
                std::static_pointer_cast<arrow::StringBuilder>(field.second)
                    ->Append(field_value));
            break;
          default:
            return arrow::Status::ExecutionError("Unexpected field type");
        }
      } else {
        ARROW_RETURN_NOT_OK(field.second->AppendNull());
      }
    }
  }

  arrow::FieldVector fields;
  arrow::ArrayVector column_arrays;

  // Creating schema and finishing builders
  column_arrays.emplace_back();
  ARROW_RETURN_NOT_OK(timestamp_builder.Finish(&column_arrays.back()));

  fields.push_back(arrow::field(time_column_name_,
                                arrow::timestamp(arrow::TimeUnit::SECOND)));

  ARROW_RETURN_NOT_OK(
      metadata::setColumnTypeMetadata(&fields.back(), metadata::TIME));

  fields.push_back(arrow::field(measurement_column_name_, arrow::utf8()));
  ARROW_RETURN_NOT_OK(
      metadata::setColumnTypeMetadata(&fields.back(), metadata::MEASUREMENT));

  column_arrays.emplace_back();
  ARROW_RETURN_NOT_OK(measurement_name_builder.Finish(&column_arrays.back()));

  for (auto& tag : tags_id_to_builders) {
    fields.push_back(arrow::field(tag.first, arrow::utf8()));
    ARROW_RETURN_NOT_OK(
        metadata::setColumnTypeMetadata(&fields.back(), metadata::TAG));

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
      default: return arrow::Status::ExecutionError("Unexpected field type");
    }

    ARROW_RETURN_NOT_OK(
        metadata::setColumnTypeMetadata(&fields.back(), metadata::FIELD));
  }

  arrow::RecordBatchVector record_batches;

  record_batches.push_back(arrow::RecordBatch::Make(
      arrow::schema(fields), parsed_metrics_.size(), column_arrays));

  ARROW_RETURN_NOT_OK(metadata::setTimeColumnNameMetadata(
      &record_batches.back(), time_column_name_));

  ARROW_RETURN_NOT_OK(metadata::setMeasurementColumnNameMetadata(
      &record_batches.back(), measurement_column_name_));

  parsed_metrics_.clear();
  return record_batches;
}

arrow::Type::type GraphiteParser::determineFieldType(
    const std::string& value) const {
  if (value == "0") {
    return arrow::Type::INT64;
  }

  char* str_end = nullptr;
  auto int64_value =
      std::strtoll(value.c_str(), &str_end, NUMBER_PARSING_BASE);
  if (errno != ERANGE && int64_value != 0 && str_end == &*value.end()) {
    return arrow::Type::INT64;
  }

  auto double_value = std::strtod(value.c_str(), &str_end);
  if (errno != ERANGE && double_value != 0 && str_end == &*value.end()) {
    return arrow::Type::DOUBLE;
  }

  return arrow::Type::STRING;
}

void GraphiteParser::parseMetricStrings(
    const std::vector<std::string>& metric_strings) {
  for (auto& metric_string : metric_strings) {
    std::shared_ptr<Metric> metric{nullptr};
    for (auto& metric_template : templates_) {
      if ((metric = metric_template.buildMetric(metric_string, separator_)) !=
          nullptr) {
        break;
      }
    }

    if (metric != nullptr) {
      auto parsed_metric_iter = parsed_metrics_.find(metric);
      if (parsed_metric_iter != parsed_metrics_.end()) {
        parsed_metric_iter->get()->mergeWith(*metric);
      } else {
        parsed_metrics_.insert(metric);
      }
    }
  }
}

GraphiteParser::Metric::Metric(
    std::string&& measurement_name,
    GraphiteParser::SortedKVContainer<std::string>&& tags)
    : measurement_name(measurement_name), tags(tags) {}

std::string GraphiteParser::Metric::getKeyString() const {
  std::vector<std::string> key_parts;
  key_parts.push_back(measurement_name);
  for (auto& tag : tags) {
    key_parts.push_back(tag.first + '=' + tag.second);
  }

  return string_utils::concatenateStrings(key_parts, ".");
}

void GraphiteParser::Metric::mergeWith(const Metric& other) {
  if (getKeyString() != other.getKeyString()) {
    return;
  }

  for (auto& field : other.fields) {
    if (fields.find(field.first) == fields.end()) {
      fields[field.first] = field.second;
    }
  }

  if (timestamp == -1) {
    timestamp = other.timestamp;
  }
}

GraphiteParser::MetricTemplate::MetricTemplate(
    const std::string& template_string) {
  auto template_string_parts = string_utils::split(template_string, " ");
  if (template_string_parts.empty()) {
    return;
  }

  size_t part_idx = 0;

  if (template_string_parts.size() == 3 ||
      (template_string_parts.size() == 2 &&
       template_string_parts[1].find('=') == std::string::npos)) {
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

std::string GraphiteParser::MetricTemplate::prepareFilterRegex(
    const std::string& filter_string) const {
  std::stringstream regex_string_builder;
  for (auto& c : filter_string) {
    switch (c) {
      case '.': regex_string_builder << "\\."; break;
      case '*': regex_string_builder << ".+"; break;
      default: regex_string_builder << c;
    }
  }

  return regex_string_builder.str();
}

const std::string GraphiteParser::MetricTemplate::MEASUREMENT_PART_ID{
    "measurement"};
const std::string GraphiteParser::MetricTemplate::FIELD_PART_ID{"field"};

void GraphiteParser::MetricTemplate::prepareTemplateParts(
    const std::string& template_string) {
  if (template_string.empty()) {
    return;
  }

  auto template_string_parts = string_utils::split(template_string, ".");
  if (template_string_parts.empty()) {
    throw GraphiteParserException();
  }

  for (int i = 0; i < template_string_parts.size() - 1; ++i) {
    addTemplatePart(template_string_parts[i]);
  }

  auto last_part = template_string_parts.back();
  multiple_last_part_ =
      !last_part.empty() && last_part.back() == '*' &&
      (last_part.substr(0, last_part.size() - 1) == MEASUREMENT_PART_ID ||
       last_part.substr(0, last_part.size() - 1) == FIELD_PART_ID);
  if (multiple_last_part_) {
    addTemplatePart(last_part.substr(0, last_part.size() - 1));
  } else {
    addTemplatePart(last_part);
  }
}

void GraphiteParser::MetricTemplate::addTemplatePart(
    const std::string& template_part) {
  if (template_part == MEASUREMENT_PART_ID) {
    parts_.push_back({TemplatePartType::MEASUREMENT});
  } else if (template_part == FIELD_PART_ID) {
    parts_.push_back({TemplatePartType::FIELD});
  } else {
    parts_.push_back({TemplatePartType::TAG, template_part});
  }
}

void GraphiteParser::MetricTemplate::prepareAdditionalTags(
    const std::string& additional_tags_string) {
  auto additional_tags_parts =
      string_utils::split(additional_tags_string, ",");
  for (auto& part : additional_tags_parts) {
    auto tag = string_utils::split(part, "=");
    if (tag.size() != 2) {
      throw GraphiteParserException();
    }

    additional_tags_[tag[0]] = tag[1];
  }
}

bool GraphiteParser::MetricTemplate::match(
    const std::string& metric_string) const {
  auto metric_parts = string_utils::split(metric_string, " ");

  if (metric_parts.size() < 2 ||
      (filter_ != nullptr && !std::regex_match(metric_parts[0], *filter_))) {
    return false;
  }

  auto metric_description_parts = string_utils::split(metric_parts[0], ".");
  return metric_description_parts.size() == parts_.size() ||
         (metric_description_parts.size() > parts_.size() &&
          multiple_last_part_);
}

const std::string GraphiteParser::MetricTemplate::DEFAULT_FIELD_NAME{"value"};

std::shared_ptr<GraphiteParser::Metric>
GraphiteParser::MetricTemplate::buildMetric(
    const std::string& metric_string, const std::string& separator) const {
  if (!match(metric_string)) {
    return nullptr;
  }

  auto metric_parts = string_utils::split(metric_string, " ");
  if (metric_parts.empty()) {
    throw GraphiteParserException();
  }

  auto metric_description_parts = string_utils::split(metric_parts[0], ".");
  if (metric_description_parts.size() < parts_.size()) {
    throw GraphiteParserException();
  }

  std::vector<std::string> measurement_name_parts;
  std::unordered_map<std::string, std::vector<std::string>> tags_parts;
  std::vector<std::string> field_parts;
  for (size_t i = 0; i < parts_.size(); ++i) {
    switch (parts_[i].type) {
      case MEASUREMENT:
        measurement_name_parts.push_back(metric_description_parts[i]);
        break;
      case FIELD: field_parts.push_back(metric_description_parts[i]); break;
      case TAG:
        if (tags_parts.find(parts_[i].id) == tags_parts.end()) {
          tags_parts.emplace(parts_[i].id, std::vector<std::string>{});
        }

        tags_parts[parts_[i].id].push_back(metric_description_parts[i]);
        break;
    }
  }

  if (metric_description_parts.size() > parts_.size() &&
      multiple_last_part_) {
    for (size_t i = parts_.size(); i < metric_description_parts.size(); ++i) {
      switch (parts_.back().type) {
        case MEASUREMENT:
          measurement_name_parts.push_back(metric_description_parts[i]);
          break;
        case FIELD: field_parts.push_back(metric_description_parts[i]); break;
        default:
          throw std::runtime_error(
              "Unexpected multiple template part with type TAG");
      }
    }
  }

  GraphiteParser::SortedKVContainer<std::string> tags;
  for (auto& tag : tags_parts) {
    tags.emplace(tag.first, std::move(string_utils::concatenateStrings(
                                tag.second, separator)));
  }

  for (auto& tag : additional_tags_) { tags.emplace(tag.first, tag.second); }

  auto metric =
      std::make_shared<Metric>(std::move(string_utils::concatenateStrings(
                                   measurement_name_parts, separator)),
                               std::move(tags));

  if (!field_parts.empty()) {
    metric->fields[string_utils::concatenateStrings(field_parts, separator)] =
        metric_parts[1];
  } else {
    metric->fields[DEFAULT_FIELD_NAME] = metric_parts[1];
  }

  char* str_end = nullptr;
  if (metric_parts.size() == 3) {
    metric->timestamp =
        std::strtoll(metric_parts[2].c_str(), &str_end, NUMBER_PARSING_BASE);
  }

  return metric;
}

bool GraphiteParser::MetricComparator::operator()(
    const std::shared_ptr<Metric>& metric1,
    const std::shared_ptr<Metric>& metric2) const {
  if (metric1->timestamp == metric2->timestamp) {
    return std::less<>()(metric1->getKeyString(), metric2->getKeyString());
  }

  if (metric1->timestamp == -1) {
    return false;
  } else if (metric2->timestamp == -1) {
    return true;
  } else {
    return metric1->timestamp < metric2->timestamp;
  }
}

}  // namespace stream_data_processor
