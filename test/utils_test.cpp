#include <functional>
#include <unordered_set>

#include <arrow/api.h>
#include <catch2/catch.hpp>

#include "test_help.h"
#include "utils/utils.h"

using namespace stream_data_processor;

TEST_CASE( "serialization and deserialization preserves schema metadata", "[Serializer]" ) {
  std::string metadata_key = "metadata";
  std::vector<std::string> metadata_values{ "batch_0", "batch_1"};

  auto metadata_0 = std::make_shared<arrow::KeyValueMetadata>();
  arrowAssertNotOk(metadata_0->Set(metadata_key, metadata_values[0]));

  auto metadata_1 = std::make_shared<arrow::KeyValueMetadata>();
  arrowAssertNotOk(metadata_1->Set(metadata_key, metadata_values[1]));

  auto field = arrow::field("field_name", arrow::int64());
  auto schema_0 = arrow::schema({field}, metadata_0);
  auto schema_1 = arrow::schema({field}, metadata_1);

  arrow::Int64Builder array_builder_0;
  arrowAssertNotOk(array_builder_0.Append(0));
  std::shared_ptr<arrow::Array> array_0;
  arrowAssertNotOk(array_builder_0.Finish(&array_0));
  auto record_batch_0 = arrow::RecordBatch::Make(schema_0, 1, {array_0});

  arrow::Int64Builder array_builder_1;
  arrowAssertNotOk(array_builder_1.Append(1));
  std::shared_ptr<arrow::Array> array_1;
  arrowAssertNotOk(array_builder_1.Finish(&array_1));
  auto record_batch_1 = arrow::RecordBatch::Make(schema_1, 1, {array_1});

  arrow::RecordBatchVector record_batches{record_batch_0, record_batch_1};
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;

  arrowAssignOrRaise(buffers, serialize_utils::serializeRecordBatches(
      record_batches));

  for (size_t i = 0; i < 2; ++i) {
    arrow::RecordBatchVector deserialized;

    arrowAssignOrRaise(
        deserialized, serialize_utils::deserializeRecordBatches(*
        buffers[i]));

    REQUIRE( deserialized.size() == 1 );
    REQUIRE( deserialized[0]->Equals(*record_batches[i], true) );
  }
}

TEST_CASE( "check conversion between different TimeUnits", "[time_utils]" ) {
  using namespace time_utils;
  constexpr int64_t max_int64 = std::numeric_limits<int64_t>::max();
  arrow::Result<int64_t> overflow_result = arrow::Status::TypeError("Overflow while converting durations");

  auto convertWithOverflowCheck = [&](int64_t t, int64_t factor) {
    return t > max_int64 / factor ? overflow_result : t * factor;
  };

  const std::unordered_map<TimeUnit, std::unordered_map<
      TimeUnit,
      std::function<arrow::Result<int64_t>(int64_t)>
      >> convert_rules {
      {NANO, {
          {NANO, [&](int64_t t) { return t; }},
          {MICRO, [&](int64_t t) { return t / 1000; }},
          {MILLI, [&](int64_t t) { return t / 1000000; }},
          {SECOND, [&](int64_t t) { return t / 1000000000; }},
          {MINUTE, [&](int64_t t) { return t / 1000000000 / 60; }},
          {HOUR, [&](int64_t t) { return t / 1000000000 / 60 / 60; }},
          {DAY, [&](int64_t t) { return t / 1000000000 / 60 / 60 / 24; }},
          {WEEK, [&](int64_t t) { return t / 1000000000 / 60 / 60 / 24 / 7; }}
      }},
      {MICRO, {
          {NANO, [&](int64_t t) { return convertWithOverflowCheck(t, 1000); }},
          {MICRO, [&](int64_t t) { return t; }},
          {MILLI, [&](int64_t t) { return t / 1000; }},
          {SECOND, [&](int64_t t) { return t / 1000000; }},
          {MINUTE, [&](int64_t t) { return t / 1000000 / 60; }},
          {HOUR, [&](int64_t t) { return t / 1000000 / 60 / 60; }},
          {DAY, [&](int64_t t) { return t / 1000000 / 60 / 60 / 24; }},
          {WEEK, [&](int64_t t) { return t / 1000000 / 60 / 60 / 24 / 7; }}
      }},
      {MILLI, {
          {NANO, [&](int64_t t) { return convertWithOverflowCheck(t, 1000000); }},
          {MICRO, [&](int64_t t) { return convertWithOverflowCheck(t, 1000); }},
          {MILLI, [&](int64_t t) { return t; }},
          {SECOND, [&](int64_t t) { return t / 1000; }},
          {MINUTE, [&](int64_t t) { return t / 1000 / 60; }},
          {HOUR, [&](int64_t t) { return t / 1000 / 60 / 60; }},
          {DAY, [&](int64_t t) { return t / 1000 / 60 / 60 / 24; }},
          {WEEK, [&](int64_t t) { return t / 1000 / 60 / 60 / 24 / 7; }}
      }},
      {SECOND, {
          {NANO, [&](int64_t t) { return convertWithOverflowCheck(t, 1000000000); }},
          {MICRO, [&](int64_t t) { return convertWithOverflowCheck(t, 1000000); }},
          {MILLI, [&](int64_t t) { return convertWithOverflowCheck(t, 1000); }},
          {SECOND, [&](int64_t t) { return t; }},
          {MINUTE, [&](int64_t t) { return t / 60; }},
          {HOUR, [&](int64_t t) { return t / 60 / 60; }},
          {DAY, [&](int64_t t) { return t / 60 / 60 / 24; }},
          {WEEK, [&](int64_t t) { return t / 60 / 60 / 24 / 7; }}
      }},
      {MINUTE, {
          {NANO, [&](int64_t t) { return convertWithOverflowCheck(t, int64_t{1000000000} * 60); }},
          {MICRO, [&](int64_t t) { return convertWithOverflowCheck(t, int64_t{1000000} * 60); }},
          {MILLI, [&](int64_t t) { return convertWithOverflowCheck(t, int64_t{1000} * 60); }},
          {SECOND, [&](int64_t t) { return convertWithOverflowCheck(t, 60); }},
          {MINUTE, [&](int64_t t) { return t; }},
          {HOUR, [&](int64_t t) { return t / 60; }},
          {DAY, [&](int64_t t) { return t / 60 / 24; }},
          {WEEK, [&](int64_t t) { return t / 60 / 24 / 7; }}
      }},
      {HOUR, {
          {NANO, [&](int64_t t) { return convertWithOverflowCheck(t, int64_t{1000000000} * 60 * 60); }},
          {MICRO, [&](int64_t t) { return convertWithOverflowCheck(t, int64_t{1000000} * 60 * 60); }},
          {MILLI, [&](int64_t t) { return convertWithOverflowCheck(t, int64_t{1000} * 60 * 60); }},
          {SECOND, [&](int64_t t) { return convertWithOverflowCheck(t, int64_t{60} * 60); }},
          {MINUTE, [&](int64_t t) { return convertWithOverflowCheck(t, 60); }},
          {HOUR, [&](int64_t t) { return t; }},
          {DAY, [&](int64_t t) { return t / 24; }},
          {WEEK, [&](int64_t t) { return t / 24 / 7; }}
      }},
      {DAY, {
          {NANO, [&](int64_t t) { return convertWithOverflowCheck(t, int64_t{1000000000} * 60 * 60 * 24); }},
          {MICRO, [&](int64_t t) { return convertWithOverflowCheck(t, int64_t{1000000} * 60 * 60 * 24); }},
          {MILLI, [&](int64_t t) { return convertWithOverflowCheck(t, int64_t{1000} * 60 * 60 * 24); }},
          {SECOND, [&](int64_t t) { return convertWithOverflowCheck(t, int64_t{60} * 60 * 24); }},
          {MINUTE, [&](int64_t t) { return convertWithOverflowCheck(t, int64_t{60} * 24); }},
          {HOUR, [&](int64_t t) { return convertWithOverflowCheck(t, 24); }},
          {DAY, [&](int64_t t) { return t; }},
          {WEEK, [&](int64_t t) { return t / 7; }}
      }},
      {WEEK, {
          {NANO, [&](int64_t t) { return convertWithOverflowCheck(t, int64_t{1000000000} * 60 * 60 * 24 * 7); }},
          {MICRO, [&](int64_t t) { return convertWithOverflowCheck(t, int64_t{1000000} * 60 * 60 * 24 * 7); }},
          {MILLI, [&](int64_t t) { return convertWithOverflowCheck(t, int64_t{1000} * 60 * 60 * 24 * 7); }},
          {SECOND, [&](int64_t t) { return convertWithOverflowCheck(t, int64_t{60} * 60 * 24 * 7); }},
          {MINUTE, [&](int64_t t) { return convertWithOverflowCheck(t, int64_t{60} * 24 * 7); }},
          {HOUR, [&](int64_t t) { return convertWithOverflowCheck(t, int64_t{24} * 7); }},
          {DAY, [&](int64_t t) { return convertWithOverflowCheck(t, 7); }},
          {WEEK, [&](int64_t t) { return t; }}
      }}
  };

  std::vector<int64_t> test_cases{0, 1, 7, 24, 60, 7 * 24, 1000, 24 * 60,
                                  24 * 60 * 60, 1000000, int64_t{1000} * 60 * 60,
                                  60 * 24 * 7, 60 * 60 * 24 * 7,
                                  1000000000, int64_t{1000} * 60 * 60 * 24 * 7,
                                  int64_t{1000000} * 60 * 60 * 24 * 7,
                                  int64_t{1000000000} * 60 * 60 * 24 * 7,
                                  int64_t{1000} * 60 * 60 * 24,
                                  int64_t{1000000} * 60 * 60 * 24,
                                  int64_t{1000000000} * 60 * 60 * 24,
                                  60 * 60, int64_t{1000000} * 60 * 60,
                                  int64_t{1000000000} * 60 * 60,
                                  1000 * 60, int64_t{1000000} * 60,
                                  int64_t{1000000000} * 60};

  std::vector<TimeUnit> time_units{NANO, MICRO, MILLI, SECOND, MINUTE, HOUR, DAY, WEEK};
  for (auto& test_case : test_cases) {
    for (auto& from : time_units) {
      for (auto& to : time_units) {
        auto convert_result = convertTime(test_case, from ,to);
        auto true_result = convert_rules.at(from).at(to)(test_case);
        REQUIRE( convert_result.status().Equals(true_result.status()) );
        if (convert_result.ok()) {
          REQUIRE( convert_result.ValueOrDie() == true_result.ValueOrDie() );
        }
      }
    }
  }
}

TEST_CASE("concatenation of empty set is empty string", "[string_utils]") {
  std::unordered_set<std::string> empty_set;
  auto concatenation_result = string_utils::concatenateStrings(empty_set);
  REQUIRE( concatenation_result.empty() );
}

TEST_CASE("concatenation of one string is exactly this string", "[string_utils]") {
  std::unordered_set<std::string> strings_set{
      "first"
  };

  auto concatenation_result =
      string_utils::concatenateStrings(strings_set, " ");

  REQUIRE( concatenation_result == "first");
}

TEST_CASE("concatenation puts delimiter right between strings", "[string_utils]") {
  std::unordered_set<std::string> strings_set{
    "first", "second"
  };

  auto concatenation_result =
      string_utils::concatenateStrings(strings_set, " ");

  REQUIRE( (concatenation_result == "first second" ||
            concatenation_result == "second first") );
}

TEST_CASE("calculating derivatives for sinus", "[FDDerivativeCalculator]") {
  size_t n = 9;
  std::deque<double> xs(n);
  std::deque<double> ys(n);
  for (int64_t i = 0; i < n; ++i) {
    xs[i] = M_PI * i / (n - 1);
    ys[i] = std::sin(xs[i]);
  }

  std::unique_ptr<sdp::compute_utils::DerivativeCalculator> derivative_calculator =
      std::make_unique<sdp::compute_utils::FDDerivativeCalculator>();

  double delta = 0.001;
  double start = 0;
  double end = M_PI;

  double required_average_error = 1e-5;
  double required_max_error = 0.0005;

  std::vector<std::function<double(double)>> true_derivative_functions{
      [](double x) { return std::sin(x); },
      [](double x) { return std::cos(x); },
      [](double x) { return - std::sin(x); }
  };

  for (size_t i = 0; i < true_derivative_functions.size(); ++i) {
    double cur_x = start;
    double max_err = 0;
    double total_err = 0;
    int64_t iters = 0;

    while (cur_x <= end) {
      auto true_der = true_derivative_functions[i](cur_x);
      auto my_der =
          derivative_calculator->calculateDerivative(xs, ys, cur_x, i);
      auto err = std::abs(true_der - my_der);
      total_err += err;
      max_err = std::max(max_err, err);
      cur_x += delta;
      ++iters;
    }

    INFO("Required average error is " << required_average_error);
    REQUIRE(total_err / iters < required_average_error);

    INFO("Required max error is " << required_max_error);
    REQUIRE(max_err < required_max_error);
  }
}

TEST_CASE("can't calculate first order derivative by one value", "[FDDerivativeCalculator]") {
  std::deque<double> xs{0};
  std::deque<double> ys{0};

  std::unique_ptr<sdp::compute_utils::DerivativeCalculator> derivative_calculator =
      std::make_unique<sdp::compute_utils::FDDerivativeCalculator>();

  REQUIRE_THROWS_AS(
      derivative_calculator->calculateDerivative(xs, ys, 0, 1),
      compute_utils::ComputeException);
}

TEST_CASE("argument and value arrays must have same sizes", "[FDDerivativeCalculator]") {
  std::deque<double> xs{0};
  std::deque<double> ys{0, 1};

  std::unique_ptr<sdp::compute_utils::DerivativeCalculator> derivative_calculator =
      std::make_unique<sdp::compute_utils::FDDerivativeCalculator>();

  REQUIRE_THROWS_AS(
      derivative_calculator->calculateDerivative(xs, ys, 0, 1),
      compute_utils::ComputeException);
}
