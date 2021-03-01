# Kapacitor UDF

## Installation

### Docker

The easiest way to run an UDF is to build and run Docker image. In this case 
you don't have to install all dependencies.

**Step 0**. Download the source code using `git clone` command.

**Step 1**. Find an UDF you want to run. All of them are placed in 
[examples](../../examples) directory. Write down the name of corresponded 
`.cpp` file. For example, if you want to run an `AggregateUDF` located in 
`/examples/aggregate_udf/aggregate_udf.cpp` file, you will need 
`aggregate_udf` name in the next step.

**Step 2**. Go to the [root](../..) directory of the project where 
[Dockerfile](../../Dockerfile) is located and build the Docker image. In the 
following command replace `<udf_name>` with UDF's name chosen in the previous 
step:

```terminal
$ cd stream-data-processor
$ docker image build \
    --target app \
    --build-arg CMAKE_BUILD_BINARY_TARGET=<udf_name> \
    -t sdp_udf .
```

**Step 3**. Run container using built image with following command. As most 
of implemented UDFs are using socket based approach do not forget to mount 
`/local/path/to/sockets` directory where all needed sockets will be located. 
Replace `<udf arguments>` with all needed command line arguments for called 
UDF.

```terminal
$ docker run --rm \
    -v /local/path/to/sockets:/container/path/to/sockets \
    sdp_udf <udf_name> <udf arguments>
```

**Step 4** (optional). As image building spawns some dangling images 
you may want to delete them to free your disk space. You can do it with 
following command (unless you have other Docker images with `project` label 
equals to `sdp`):

```terminal
$ docker image prune --filter "label=project=sdp" --force
```

#### Hint

You may notice that the image building step takes a lot of time. If you want 
to skip the stage of system configuration you can pre-build corresponding 
image so Docker will use it for every new iteration of UDF building:

```terminal
$ docker image build \
    --target system-config \
    -t sdp_system_config .
```

### Build from source

#### Requirements

* g++ or clang
* cmake version 3.13 or higher
* git
* [protobuf](https://developers.google.com/protocol-buffers)
* [llvm](https://llvm.org)
* re2
* boost
* [Apache Arrow](https://arrow.apache.org/install/) version 3.0.0 or higher 
  with Gandiva expression compiler. You can refer to 
  [Dockerfile](../../Dockerfile) to see how to build it from source with all 
  needed components
* [spdlog](https://github.com/gabime/spdlog)
* [zeromq](https://zeromq.org) with [cppzmq](https://github.com/zeromq/cppzmq)

#### Build

* Download the source code using `git clone` command
* `cd stream-data-processor`
* `mkdir build && cd build`
* `cmake .. -DENABLE_TESTS=OFF -DCLANG_TIDY_LINT=OFF -DBUILD_SHARED_LIBS=OFF`
* `make <target>`
* Executable file is located at the `bin` directory: `./bin/<target> --help`

Available UDF targets:

* `aggregate_udf`
* `dynamic_window_udf`
* `threshold_udf`

### User Guide

#### UDFs docs

You can find the documentation and examples of usage of UDFs in corresponded 
directories:

* [AggregateUDF](../../examples/aggregate_udf)
* [DynamicWindowUDF](../../examples/dynamic_window_udf)
* [ThresholdUDF](../../examples/threshold_udf)

#### Kapacitor configuration

Do not forget to add all necessary 
[UDF settings](https://docs.influxdata.com/kapacitor/v1.5/guides/socket_udf/#configure-kapacitor-to-talk-to-the-udf) 
to your Kapacitor configuration file. Note that we are using socket based 
approach. For example, if you are using AggregateUDF, your configuration 
file should include something like (paths can differ):

```
[udf]
[udf.functions]
    [udf.functions.batchAggregateUDF]
        socket = "/var/run/batchAggregateUDF.sock"
        timeout = "10s"

    [udf.functions.streamAggregateUDF]
        socket = "/var/run/streamAggregateUDF.sock"
        timeout = "10s"
```

In this case your call of AggregateUDF will look like:

```terminal
$ aggregate_udf --batch /var/run/batchAggregateUDF.sock --stream /var/run/streamAggregateUDF.sock
```

## Implementation

SDP library is implementing [`IUDFAgent`](udf_agent.h) for communicating with 
Kapacitor via Kapacitor's UDF RPC protocol. UDFs use 
[socket based approach](https://github.com/influxdata/kapacitor/tree/master/udf/agent#child-process-vs-socket) 
to communicate with Kapacitor, but `UDFAgent` is ready to work with child 
process based approach if needed.

We use [uvw library](https://uvw.docsforge.com), which is 
[libuv](https://github.com/libuv/libuv) wrapper, in order to provide 
asynchronous I/O. It also allows to write code in event-based approach.

Every UDF requires separate 
[`RequestHandler`](request_handlers/request_handler.h) 
interface implementation -- it handles incoming RPC calls from Kapacitor and 
sends responses back via `IUDFAgent`.

As long as we are using Apache Arrow library for data processing we need to 
**store**, **handle** data and **convert** it between Kapacitor's points and 
Arrow's `RecordBatch`es formats. Currently this functionality is provided by 
corresponding objects (we are working at moving storing functionality to 
the separate `IPointsStorage` interface):

* [`RecordBatchRequestHandler`](request_handlers/record_batch_request_handler.h)
  for storing (moving to `IPointsStorage` in progress);
* [`RecordBatchHandler`](../record_batch_handlers) from the main part of 
  the library for data handling;
* [`PointsConverter`](utils/points_converter.h) for data converting.

Therefore, the most important part of every UDF is a 
`RecordBatchHandler` that is doing all useful work. See 
[full list](../../README.md#RecordBatchHandler) of currently implemented 
`RecordBatchHandler`s.
