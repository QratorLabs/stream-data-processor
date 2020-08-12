#include <chrono>
#include <iostream>
#include <memory>
#include <string>

#include <uvw.hpp>

#include <zmq.hpp>

#include "utils/utils.h"

/* Proof that uvw::PollHandle doesn't work with zmq sockets */
int main(int argc, char** argv) {
  auto loop = uvw::Loop::getDefault();
  zmq::context_t zmq_context(1);

  zmq::socket_t pub_socket(zmq_context, ZMQ_PUB);
  pub_socket.bind("inproc://experiments");

  zmq::socket_t sub_socket(zmq_context, ZMQ_SUB);
  sub_socket.setsockopt(ZMQ_SUBSCRIBE, "", 0);
  sub_socket.connect("inproc://experiments");

  TransportUtils::send(pub_socket, "test 1");
  TransportUtils::send(pub_socket, "test 2");
  auto test_message = TransportUtils::receive(sub_socket);
  std::cerr << test_message << std::endl;

  size_t i = 0;

  auto pub_poller = loop->resource<uvw::PollHandle>(pub_socket.getsockopt<int>(ZMQ_FD));
  auto sub_poller = loop->resource<uvw::PollHandle>(sub_socket.getsockopt<int>(ZMQ_FD));

  pub_poller->on<uvw::PollEvent>([&](const uvw::PollEvent&, uvw::PollHandle&) {
    if (pub_socket.getsockopt<int>(ZMQ_EVENTS) & ZMQ_POLLOUT) {
      if (!TransportUtils::send(pub_socket, std::to_string(i))) {
        pub_poller->close();
        sub_poller->close();
        throw std::runtime_error("Error " + std::to_string(zmq_errno()));
      } else {
        std::cerr << i << " was sent\n";
        if (i == 100) {
          pub_poller->close();
          /* Without this line the script runs eternally because uvw doesn't emit PollEvent for subscriber socket
           * until publisher socket is stopped */
          // pub_socket.close();
        }

        ++i;
      }
    }
  });
  pub_poller->start(uvw::Flags<uvw::PollHandle::Event>::from<uvw::PollHandle::Event::WRITABLE>());

  sub_poller->on<uvw::PollEvent>([&](const uvw::PollEvent&, uvw::PollHandle&) {
    std::cerr << "Polled subscriber\n";
    while (sub_socket.getsockopt<int>(ZMQ_EVENTS) & ZMQ_POLLIN) {
      auto message = TransportUtils::receive(sub_socket);
      std::cerr << message << std::endl;
      if (message == "100") {
        sub_poller->close();
      }
    }
  });
  sub_poller->start(uvw::Flags<uvw::PollHandle::Event>::from<uvw::PollHandle::Event::READABLE>());

  try {
    loop->run();
  } catch (...) {
    pub_socket.close();
    sub_socket.close();
  }

  pub_socket.close();
  sub_socket.close();

  return 0;
}