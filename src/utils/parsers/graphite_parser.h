#pragma once

#include <map>
#include <memory>
#include <regex>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/stl_allocator.h>

#include "parser.h"

class GraphiteParser : public Parser {
 public:
  explicit GraphiteParser(const std::vector<std::string>& template_strings, std::string separator = ".");

  arrow::Status parseRecordBatches(const std::shared_ptr<arrow::Buffer>& buffer,
                                   std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches) override;

 private:
  void parseMetricStrings(const std::vector<std::string>& metric_strings);
  [[nodiscard]] arrow::Type::type determineFieldType(const std::string& value) const;

 private:
  template<typename T>
  using KVContainer = std::unordered_map<
      std::string,
      T,
      std::hash<std::string>,
      std::equal_to<>,
      arrow::stl::allocator<std::pair<const std::string, T>>
  >;

  template<typename T>
  using SortedKVContainer = std::map<
      std::string,
      T,
      std::less<>,
      arrow::stl::allocator<std::pair<const std::string, T>>
  >;

  class Metric {
   public:
    explicit Metric(std::string&& measurement_name, SortedKVContainer<std::string>&& tags = { });

    [[nodiscard]] std::string getKeyString() const;

    void mergeWith(const Metric& other);

    std::string measurement_name_;
    SortedKVContainer<std::string> tags_;
    KVContainer<std::string> fields_;
  };

  class MetricTemplate {
   public:
    explicit MetricTemplate(const std::string& template_string);

    [[nodiscard]] bool match(const std::string& metric_string) const;
    [[nodiscard]] std::shared_ptr<Metric> buildMetric(const std::string &metric_string,
                                                      const std::string &separator) const;

   private:
    [[nodiscard]] std::string prepareFilterRegex(const std::string& filter_string) const;
    void prepareTemplateParts(const std::string& template_string);
    void prepareAdditionalTags(const std::string& additional_tags_string);

    void addTemplatePart(const std::string& part_string);

   private:
    enum TemplatePartType {
      MEASUREMENT,
      TAG,
      FIELD
    };

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

  std::string separator_;
  std::vector<MetricTemplate> templates_;
  KVContainer<std::shared_ptr<Metric>> parsed_metrics_;
};


