#include <fstream>

#include <gandiva/tree_expr_builder.h>

#include <spdlog/spdlog.h>

#include <uvw.hpp>

#include "data_handlers/data_handlers.h"

#include "nodes/eval_node.h"
#include "nodes/finalize_node.h"

int main() {
  spdlog::set_level(spdlog::level::debug);

  auto loop = uvw::Loop::getDefault();

  std::ofstream oss("result.txt");
  FinalizeNode finalize_node("finalize_node", loop, {"127.0.0.1", 4250}, oss);

  std::vector<std::string> aggregate_columns({"tag"});
  AggregateHandler::AggregateOptions aggregate_options{{{"sum", {"first", "last", "min", "max"}},
                                                        {"diff", {"first", "last", "min", "max"}}},
                                                       true};
  EvalNode aggregate_node("aggregate_node", loop,
                          std::make_shared<AggregateHandler>(aggregate_columns,
                                                             aggregate_options,
                                                             "ts"),
                          {"127.0.0.1", 4242},
                          {
                         {"127.0.0.1", 4250}
                     });

  auto field_ts = arrow::field("ts", arrow::timestamp(arrow::TimeUnit::SECOND));
  auto field0 = arrow::field("operand1", arrow::int64());
  auto field1 = arrow::field("operand2", arrow::int64());
  auto field2 = arrow::field("tag", arrow::utf8());
  auto schema = arrow::schema({field_ts, field0, field1, field2});

  auto field_sum = field("sum", arrow::int64());
  auto field_diff = field("diff", arrow::int64());

  auto sum_expr = gandiva::TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
  auto subtract_expr = gandiva::TreeExprBuilder::MakeExpression("subtract", {field0, field1}, field_diff);

  gandiva::ExpressionVector expressions{sum_expr, subtract_expr};

  EvalNode eval_node("eval_node", loop,
      std::make_shared<MapHandler>(schema, expressions),
      {"127.0.0.1", 4241},
      {
    {"127.0.0.1", 4242}
      });

  EvalNode pass_node("pass_node", loop,
      std::make_shared<CSVToRecordBatchesConverter>(),
          {"127.0.0.1", 4240},
          {
    {"127.0.0.1", 4241}
          });

  loop->run();

  oss.close();

  return 0;
}