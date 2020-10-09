#pragma once

#include <fstream>
#include <string>

#include <arrow/api.h>

#include "consumer.h"

class PrintConsumer : public Consumer {
 public:
  explicit PrintConsumer(std::ofstream& ostrm);

  void start() override;
  void consume(const char* data, size_t length) override;
  void stop() override;

 private:
  void printRecordBatch(const arrow::RecordBatch& record_batch);

 private:
  std::ofstream& ostrm_;
};

class FilePrintConsumer : public PrintConsumer {
 public:
  explicit FilePrintConsumer(const std::string& file_name);
  ~FilePrintConsumer();

 private:
  std::ofstream ostrm_obj_;
};
