#pragma once

#include <chrono>
#include <ostream>

#include "server/unix_socket_client.h"

namespace benchmarks {

class DataGenerator {
 public:
  explicit DataGenerator(std::ostream& output_stream)
      : output_stream_(output_stream) {
  }

  virtual void start() = 0;
  virtual void stop() = 0;

 protected:
  std::ostream& output_stream_;
};

namespace sdp = stream_data_processor;

class KapacitorPointsGenerator : public DataGenerator {
 public:
  explicit KapacitorPointsGenerator(std::ostream& output_stream);

  void start() override;
  void stop() override;

 private:

};

} // namespace benchmarks
