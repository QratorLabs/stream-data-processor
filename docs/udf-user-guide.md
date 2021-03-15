# Kapacitor UDF User Guide

## Usage examples

You can find the UDFs' documentation and usage examples in corresponded
directories:

* [AggregateUDF](../examples/aggregate_udf)
* [DynamicWindowUDF](../examples/dynamic_window_udf)
* [ThresholdUDF](../examples/threshold_udf)

## How to run it

### Docker

The easiest way to run an UDF is to build and run a Docker image. In this 
case you don't have to install all dependencies.

**Step 0**. Download the source code using `git clone` command.

**Step 1**. Find an UDF you want to run. All of them are placed in
[examples](../examples) directory. Write down the name of the corresponded
`.cpp` file. For example, if you want to run AggregateUDF located in the 
`/examples/aggregate_udf/aggregate_udf.cpp` file, you will need 
`aggregate_udf` name in the next step. Available UDF names are:

* `aggregate_udf`
* `dynamic_window_udf`
* `threshold_udf`

**Step 2**. Go to the [root](..) directory of the project where the
[Dockerfile](../Dockerfile) is located and build the Docker image. In the
following command replace `<udf_name>` with UDF name chosen in the previous
step:

```terminal
$ cd stream-data-processor
$ docker image build \
    --target app \
    --build-arg CMAKE_BUILD_BINARY_TARGET=<udf_name> \
    -t sdp_udf .
```

**Step 3**. Configure your Kapacitor instance. Do not forget to add all 
necessary [UDF settings](https://docs.influxdata.com/kapacitor/v1.5/guides/socket_udf/#configure-kapacitor-to-talk-to-the-udf) 
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

**Step 4**. Run container using built image with following command. As the 
most of implemented UDFs are using socket based approach do not forget to 
mount `/local/path/to/sockets` directory where all needed sockets will be 
located. Replace `<udf arguments>` with all needed command line arguments for 
called UDF.

```terminal
$ docker run --rm \
    -v /local/path/to/sockets:/container/path/to/sockets \
    sdp_udf /app/<udf_name> <udf arguments>
```

Note that every UDF executable file provides short "help" message by adding 
`--help` flag. For example, if `<udf_name>` is `aggragate_udf`:

```terminal
$ docker run --rm sdp_udf /app/aggregate_udf --help
Aggregates data from kapacitor
Usage:
  AggregateUDF [OPTION...]

  -b, --batch arg   Unix socket path for batch data
  -s, --stream arg  Unix socket path for stream data
  -v, --verbose     Enable detailed logging
  -h, --help        Print this message
```

**Step 5** (optional). As image building spawns some dangling images
you may want to delete them to free your disk space. You can do it with
following command:

```terminal
$ docker image prune \
    --filter "label=project=sdp" \
    --filter "label=stage=builder" --force
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

In case you want to build UDF from source please refer to the
[building from source documentation](build-from-source.md).

### What's next?

* You can [write your own UDF](udf-implementation.md#Writing your own UDF) if 
  none of presented satisfy your needs.
* Take a look at the [Computation graphs](computation-graph.md) part of the 
  library
