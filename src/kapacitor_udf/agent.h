#pragma once

#include <istream>
#include <memory>
#include <mutex>
#include <thread>

#include "request_handlers/request_handler.h"

#include "udf.pb.h"

class Agent {
 public:
  Agent(std::istream& in, std::ostream& out);

  void setHandler(const std::shared_ptr<RequestHandler>& request_handler);

  void start();
  void wait();
  void writeResponse(const agent::Response& response);

 private:
  void readLoop();

 private:
  std::istream& in_;
  std::ostream& out_;
  std::shared_ptr<RequestHandler> request_handler_;
  std::mutex write_mutex_;
  std::thread read_loop_thread_;
};


