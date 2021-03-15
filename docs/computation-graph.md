# Computation graph

## How it works

All data passing through this graph is represented as `arrow::RecordBatch` - 
a columnar format of Apache Arrow library. It has typed fields (columns) and
records (rows), can be serialized and deserialized from `arrow::Buffer` - an
object containing a pointer to a piece of contiguous memory with a particular
size.

The main unit of the library is `NodePipeline` consisting of three parts:
`Producer`, `Node` and number of `Consumers`. In the certain pipeline
`Producer` provides data for `Node` from some kind of data source: external
TCP endpoint, another pipeline, etc. After that, `Node` is responsible for
handling the data according to the `Node`'s type. And finally, `Node` passes
the handled data to `Consumers`, which send it to the next pipeline or write
it to the file or something else.

Such design were used to separate parts that are responsible for data
handling and data transfer. It allows user to create very flexible and
configurable computation graph.

### Node

The `Node` is used to mutate data as soon as it arrives. It doesn't have
any internal state so it is easy to understand how it works. The `EvalNode`
uses provided in the constructor `DataHandler` to handle arriving data. There
are two types of data handlers that are currently implemented:
- `DataParser` - parses data arriving in the certain format. For example,
  CSV or Graphite output data format.
- `SerializedRecordBatchHandler` - deserialize arriving data from
  `arrow::Buffer` to the vector of `arrow::RecordBatch` that can be handled
  by provided `RecordBatchHandler`.

### RecordBatchHandler

There is a full list of currently available handlers:
- `AggregateHandler` - aggregates data using provided aggregate functions
  (*first*, *last*, *mean*, *min*, *max*).
- `DefaultHandler` - sets default values for columns. Analog of the
  Kapacitor node of the same name.
- `FilterHandler` - filters rows with provided conditions. Use
  `arrow::gandiva` library to create conditions tree.
- `GroupHandler` - splits record batches into groups with the same values in
  columns.
- `MapHandler` - evaluates expressions with present columns as arguments.
  Use `arrow::gandiva` library to create expressions.
- `SortHandler` - sorts rows by the certain column.
- `JoinHandler` - joins received record batches on the set of columns.
- `WindowHandler` - analogue of Kapacitor WindowNode.
- `ThresholdStateMachine` - sets a threshold level adjusting it to the
  incoming data.
- `GroupDispatcher` - splits incoming data into groups according to metadata
  and uses another `RecordBatchHandler` type to handle each group separately.
- `LogHandler` - logs incoming data using
  [spdlog](https://github.com/gabime/spdlog) library.

### Producer

`Producer` provides data to the certain `Node`. There are two types of data
producers have been implemented:
- `TCPProducer` - listens on the certain endpoint for arriving data. It is
  mostly used to receive data from the external data source.
- `SubscriberProducer` - producer based on ZeroMQ PUB-SUB pattern. Created
  for transferring data between pipelines. As argument it takes
  `transport_utils::Subscriber` class containing two ZMQ sockets: subscriber
  socket and synchronize socket. It needs for proper PUB-SUB communicating
  (for more details see
  [ZMQ Guide](http://zguide.zeromq.org/page:chapter2#Node-Coordination)).

### Consumer

As opposite to `Producer` this class consumes data from `Node` and pass it to
the next destination. Available types of consumer:
- `PrintConsumer` - write record batches to the output stream.
  `PrintFileConsumer` subclass is more convenient way of writing to the file.
- `TCPConsumer` - writes data to the TCP socket.
- `PublisherConsumer` - the second part of PUB-SUB pattern.

### Helpers

- As configuring PUB-SUB consumers and producers appears to be unhandy and
  massive the `NodePipeline::subscribeTo` method was implemented. It can be
  used after nodes of two pipelines have been set to create
  `PublisherConsumer`/`SubscriberProducer` pair for these pipelines without
  manual creating ZMQ sockets.
- [src/utils](src/utils) directory is full of useful instruments if you are going to
  implement some additional functionality by yourself.