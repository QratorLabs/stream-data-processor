# Build from source

## Requirements

* g++ or clang
* cmake version 3.13 or higher
* git
* [protobuf](https://developers.google.com/protocol-buffers)
* [llvm](https://llvm.org)
* re2
* boost
* [Apache Arrow](https://arrow.apache.org/install/) version 3.0.0 or higher
  with Gandiva expression compiler. You can refer to
  [Dockerfile](../Dockerfile) to see how to build it from source with all
  needed components
* [spdlog](https://github.com/gabime/spdlog)
* [zeromq](https://zeromq.org) with [cppzmq](https://github.com/zeromq/cppzmq)

## Build

* Download the source code using `git clone` command
* `cd stream-data-processor`
* `mkdir build && cd build`
* `cmake ..`
* `make <target>`
* Executable file is located at the `bin` directory: `./bin/<target> --help`

### Some instruments for development

* `cmake` flag `-DENABLE_TESTS=ON` enables tests building
* Running the [apply_clang_format.sh](../apply_clang_format.sh) script is 
  recommended before creating a Pull Request
* [test_script.sh](../test_script.sh) allows you to run unit tests and lint 
  checkers
