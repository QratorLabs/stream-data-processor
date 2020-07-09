#include <fstream>

#include <uvw.hpp>

#include "csv_to_record_batches_converter.h"
#include "eval_node.h"
#include "finalize_node.h"

int main() {
  auto loop = uvw::Loop::getDefault();

  std::ofstream oss("result.txt");
  FinalizeNode finalize_node(loop, {"127.0.0.1", 4250}, oss);

  EvalNode pass_node(loop,
      std::make_shared<CSVToRecordBatchesConverter>(true),
          {"127.0.0.1", 4240},
          {
    {"127.0.0.1", 4250}
          });

  loop->run();

  oss.close();

  return 0;
}