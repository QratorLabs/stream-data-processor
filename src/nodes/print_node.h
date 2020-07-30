#pragma once

#include <fstream>
#include <memory>
#include <string>

#include <arrow/api.h>

#include "uvw.hpp"

#include "node_base.h"

class PrintNode : public NodeBase {
 public:
  PrintNode(std::string name,
            const std::shared_ptr<uvw::Loop>& loop,
            const IPv4Endpoint& listen_endpoint,
            std::ofstream& ostrm);

 private:
  void configureServer();

  void writeData();
  void stop();

  void writeRecordBatch(const std::shared_ptr<arrow::RecordBatch>& record_batch);

 private:
  std::shared_ptr<arrow::BufferBuilder> buffer_builder_;
  std::ofstream& ostrm_;
};


