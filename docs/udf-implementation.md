# UDF Implementation

## Library architecture

SDP library is implementing [`IUDFAgent`](../src/kapacitor_udf/udf_agent.h) 
for communicating with Kapacitor via Kapacitor's UDF RPC protocol. UDFs use
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
  for storing (moving to `IPointsStorage` is in progress);
* [`RecordBatchHandler`](../src/record_batch_handlers) from the main part of
  the library for data handling;
* [`PointsConverter`](../src/kapacitor_udf/utils/points_converter.h) for data
  converting.

Therefore, the most important part of every UDF is a
`RecordBatchHandler` that is doing all useful work. See the 
[full list](computation-graph.md#RecordBatchHandler) of currently implemented
`RecordBatchHandler`s.

## Writing your own UDF

If you want to write your own UDF using SDP library you could follow the 
basic steps. As example, you can refer to 
[`streamAggregateUDF`'s](../src/kapacitor_udf/request_handlers/aggregate_request_handlers/stream_aggregate_request_handler.h) 
and [`batchAggregateUDF`'s](../src/kapacitor_udf/request_handlers/aggregate_request_handlers/batch_aggregate_request_handler.h) 
`RequestHandeler` implementations (items 3, 4, 6, 7) and to the 
[AggregateUDF's `.cpp` file with `main`](../examples/aggregate_udf/aggregate_udf.cpp) 
(item 8).

1. Check out if your UDF is expressed through one of 
   [`RecordBatchHandler`s](computation-graph.md#RecordBatchHandler) or their 
   composition. If it's not then implement new `RecordBatchHandler` that has 
   minimal missing functionality for your UDF.
2. Decide which type does your UDF have: does it consume stream or batch 
   data? Choose appropriate base class for your UDF's `RequestHandler`: for 
   **wants-batch** you should probably use 
   [`RecordBatchRequestHandler`](../src/kapacitor_udf/request_handlers/record_batch_request_handler.h) 
   (creating separate base class for this wants-batch `RequestHandler` type 
   is in progress), for **wants-stream** one of 
   [`StreamRecordBatchRequestHandlerBase`](../src/kapacitor_udf/request_handlers/record_batch_request_handler.h) 
   and [`TimerRecordBatchRequestHandlerBase`](../src/kapacitor_udf/request_handlers/record_batch_request_handler.h) 
   should be suitable for you.
3. Decide which parameters your UDF should have in terms of [Kapacitor RPC 
   Protocol](../src/kapacitor_udf/udf.proto). Implement 
   `RequestHandler::info` method according to UDF's parameters and type.
4. Create instrument for parsing UDF's parameters from init RPC message.
5. If you need additional pre-processing for converting Kapacitor's points to
   Arrow's `RecordBatch`es, you can implement your own 
   [`PointsConverter`](../src/kapacitor_udf/utils/points_converter.h) 
   decorator.
6. Implement `RequestHandler::init` method. There you should (design changes 
   are in progress):
   * use your parameters parser from item 3 to parse `agent::InitMessage` RPC
     message;
   * set `RecordBatchHandler` from item 1 according to your UDF's 
     functionality using protected `RecordBatchRequestHandler::setHandler` 
     method;
   * set `PointsConverter` with possibly implemented decorator from item 5 
     using protected `RecordBatchRequestHandler::setPointsConverter` method.
7. Implement other missing methods if any: `RequestHandler::snapshot`, 
   `RequestHandler::restore`, `RequestHandler::beginBatch`, 
   `RequestHandler::point`, `RequestHandler::endBatch` (in the future they 
   all will be predefined, design changes are in progress);
8. Implement [`UnixSocketClientFactory`](../src/server/unix_socket_client.h)
   that will be creating [`AgentClient`s](../src/kapacitor_udf/udf_agent.h) 
   with `UDFAgent` and just implemented `RequestHandler` in it. Use this 
   factory to generate connections to 
   [`UnixSocketServer`](../src/server/unix_socket_server.h).
