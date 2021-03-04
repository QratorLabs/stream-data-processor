# UDF Implementation

## Library architecture

SDP library is implementing [`IUDFAgent`](udf_agent.h) for communicating with
Kapacitor via Kapacitor's UDF RPC protocol. UDFs use
[socket based approach](https://github.com/influxdata/kapacitor/tree/master/udf/agent#child-process-vs-socket)
to communicate with Kapacitor, but `UDFAgent` is ready to work with child
process based approach if needed.

We use [uvw library](https://uvw.docsforge.com), which is
[libuv](https://github.com/libuv/libuv) wrapper, in order to provide
asynchronous I/O. It also allows to write code in event-based approach.

Every UDF requires separate
[`RequestHandler`](../src/kapacitor_udf/request_handlers/request_handler.h)
interface implementation -- it handles incoming RPC calls from Kapacitor and
sends responses back via `IUDFAgent`.

As long as we are using Apache Arrow library for data processing we need to
**store**, **handle** data and **convert** it between Kapacitor's points and
Arrow's `RecordBatch`es formats. Currently this functionality is provided by
corresponding objects (we are working at moving storing functionality to
the separate `IPointsStorage` interface):

* [`RecordBatchRequestHandler`](../src/kapacitor_udf/request_handlers/record_batch_request_handler.h)
  for storing (moving to `IPointsStorage` in progress);
* [`RecordBatchHandler`](../src/record_batch_handlers) from the main part of
  the library for data handling;
* [`PointsConverter`](../src/kapacitor_udf/utils/points_converter.h) for data
  converting.

Therefore, the most important part of every UDF is a
`RecordBatchHandler` that is doing all useful work. See
[full list](../README.md#RecordBatchHandler) of currently implemented
`RecordBatchHandler`s.
