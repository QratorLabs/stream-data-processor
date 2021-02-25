#include "rw_utils.h"
#include "utils/uvarint_utils.h"

namespace stream_data_processor {
namespace kapacitor_udf {
namespace rw_utils {

IKapacitorRequestReader::~IKapacitorRequestReader() = default;

void KapacitorRequestReader::readRequests(
    std::istream& input_stream,
    const std::function<void(const agent::Request&)>& handler) {
  agent::Request request;
  try {
    while (true) {
      uint32_t request_size = 0;
      if (residual_request_size_ == 0) {
        request_size = uvarint_utils::decode(input_stream);
      } else {
        request_size = residual_request_size_;
        residual_request_size_ = 0;
      }

      std::string request_data;
      request_data.resize(request_size);
      input_stream.read(request_data.data(), request_size);
      if (!residual_request_data_.empty()) {
        residual_request_data_.append(request_data);
        request_data = std::move(residual_request_data_);
        residual_request_data_.clear();
      }

      if (input_stream.gcount() < request_size) {
        residual_request_size_ = request_size - input_stream.gcount();
        residual_request_data_ =
            std::move(request_data.substr(0, input_stream.gcount()));
        return;
      }

      request.ParseFromString(request_data);
      handler(request);
    }
  } catch (const uvarint_utils::EOFException&) {}
}

void writeToStreamWithUvarintLength(std::ostream& output_stream,
                                    const std::string& str) {
  uvarint_utils::encode(output_stream, str.size());
  output_stream << str;
}

}  // namespace rw_utils
}  // namespace kapacitor_udf
}  // namespace stream_data_processor
