#pragma once

#include <ctime>
#include <exception>
#include <map>
#include <memory>
#include <regex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/stl_allocator.h>

#include "parser.h"

namespace stream_data_processor {

class GraphiteParserException : public std::exception {};

class GraphiteParser : public Parser {
 public:
  struct GraphiteParserOptions {
    std::vector<std::string> template_strings;
    std::string time_column_name{"time"};
    std::string separator{"."};
    std::string measurement_column_name{"measurement"};
  };

  explicit GraphiteParser(const GraphiteParserOptions& parser_options);

  [[nodiscard]] arrow::Result<arrow::RecordBatchVector> parseRecordBatches(
      const arrow::Buffer& buffer) override;

 private:
  void parseMetricStrings(const std::vector<std::string>& metric_strings);
  [[nodiscard]] arrow::Type::type determineFieldType(
      const std::string& value) const;

 private:
  template <typename T>
  using KVContainer = std::unordered_map<
      std::string, T, std::hash<std::string>, std::equal_to<>,
      arrow::stl::allocator<std::pair<const std::string, T>>>;

  template <typename T>
  using SortedKVContainer =
      std::map<std::string, T, std::less<>,
               arrow::stl::allocator<std::pair<const std::string, T>>>;

  class Metric {
   public:
    explicit Metric(std::string&& measurement_name,
                    SortedKVContainer<std::string>&& tags = {});

    [[nodiscard]] std::string getKeyString() const;

    void mergeWith(const Metric& other);

    std::string measurement_name;
    SortedKVContainer<std::string> tags;
    KVContainer<std::string> fields;
    std::time_t timestamp{-1};
  };

  class MetricComparator {
   public:
    bool operator()(const std::shared_ptr<Metric>& metric1,
                    const std::shared_ptr<Metric>& metric2) const;
  };

  class MetricTemplate {
   public:
    explicit MetricTemplate(const std::string& template_string);

    [[nodiscard]] bool match(const std::string& metric_string) const;
    [[nodiscard]] std::shared_ptr<Metric> buildMetric(
        const std::string& metric_string, const std::string& separator) const;

   private:
    [[nodiscard]] std::string prepareFilterRegex(
        const std::string& filter_string) const;
    void prepareTemplateParts(const std::string& template_string);
    void prepareAdditionalTags(const std::string& additional_tags_string);

    void addTemplatePart(const std::string& part_string);

   private:
    enum TemplatePartType { MEASUREMENT, TAG, FIELD };

    struct TemplatePart {
      TemplatePartType type;
      std::string id;
    };

    static const std::string MEASUREMENT_PART_ID;
    static const std::string FIELD_PART_ID;
    static const std::string DEFAULT_FIELD_NAME;

    std::shared_ptr<std::regex> filter_{nullptr};
    std::vector<TemplatePart> parts_;
    std::unordered_map<std::string, std::string> additional_tags_;
    bool multiple_last_part_{false};
  };

 private:
  const static int NUMBER_PARSING_BASE{10};

  std::string separator_;
  std::string time_column_name_;
  std::string measurement_column_name_;
  std::vector<MetricTemplate> templates_;
  std::set<std::shared_ptr<Metric>, MetricComparator> parsed_metrics_;
};

}  // namespace stream_data_processor
