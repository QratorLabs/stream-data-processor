# Kapacitor UDF

## User guide

### Docker

The easiest way to run an UDF is to build and run Docker image. In this case 
you don't have to install all dependencies.

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

## Implementation

SDP library is implementing `IUDFAgent` for communicating with Kapacitor via
uses Kapacitor's UDF RPC protocol. UDFs use 
[socket based approach](https://github.com/influxdata/kapacitor/tree/master/udf/agent#child-process-vs-socket) 
to communicate with Kapacitor.
