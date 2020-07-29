#include "node_base.h"

#include <spdlog/spdlog.h>

NodeBase::NodeBase(std::string name, const std::shared_ptr<uvw::Loop> &loop, const IPv4Endpoint &listen_endpoint)
    : name_(std::move(name))
    , logger_(spdlog::basic_logger_mt(name_, "logs/" + name_ + ".txt", true))
    , server_(loop->resource<uvw::TCPHandle>()) {
  server_->bind(listen_endpoint.host, listen_endpoint.port);
  server_->listen();
  spdlog::get(name_)->info("Node started on {}:{}", listen_endpoint.host, listen_endpoint.port);
}
