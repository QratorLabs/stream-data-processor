#pragma once

#include <functional>
#include <istream>
#include <ostream>

#include "udf.pb.h"

namespace stream_data_processor {
namespace kapacitor_udf {
namespace rw_utils {

class IKapacitorRequestReader {
 public:
  virtual void readRequests(
      std::istream& input_stream,
      const std::function<void(const agent::Request&)>& handler) = 0;

  virtual ~IKapacitorRequestReader() = 0;

 protected:
  IKapacitorRequestReader() = default;

  IKapacitorRequestReader(const IKapacitorRequestReader& /* non-used */) =
      default;
  IKapacitorRequestReader& operator=(
      const IKapacitorRequestReader& /* non-used */) = default;

  IKapacitorRequestReader(IKapacitorRequestReader&& /* non-used */) = default;
  IKapacitorRequestReader& operator=(
      IKapacitorRequestReader&& /* non-used */) = default;
};

class KapacitorRequestReader : public IKapacitorRequestReader {
 public:
  void readRequests(
      std::istream& input_stream,
      const std::function<void(const agent::Request&)>& handler) override;

 private:
  std::string residual_request_data_;
  size_t residual_request_size_{0};
};

void writeToStreamWithUvarintLength(std::ostream& output_stream,
                                    const std::string& str);

}  // namespace rw_utils
}  // namespace kapacitor_udf
}  // namespace stream_data_processor
