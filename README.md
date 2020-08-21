# Stream data processor library

This library provides instruments to create distributed asynchronous acyclic computation graph to process continuous
stream of arriving data. All data passing through this graph is represented as `arrow::RecordBatch` - a columnar format 
of Apache Arrow library. It has typed fields (columns) and records (rows), can be serialized and deserialized from
`arrow::Buffer` - an object containing a pointer to a piece of contiguous memory with a particular size.

The main unit of the library is `NodePipeline` consisting of three parts: `Producer`, `Node` and number of
`Consumers`. In the certain pipeline `Producer` provides data for `Node` from some kind of data source: external
TCP endpoint, another pipeline, etc. After that, `Node` is responsible for handling the data according to the `Node`'s
type. And finally, `Node` passes the handled data to `Consumers`, which send it to the next pipeline or write it to the
file or something else.

Such design were used to separate parts which are responsible for data handling and data transfer. This allows user
to create very flexible and configurable computation graph.

## `Node`

Currently there are only two types of nodes:
 - EvalNode
 - PeriodNode
 
### `EvalNode`

This node type is used to mutate data as soon as it arrives. It doesn't have any internal state so it is easy to
understand how it works. The `EvalNode` uses provided in constructor `DataHandler` to handle arriving data. There is a
full list of data handlers that are currently implemented:
 - `AggregateHandler` - aggregates data using provided aggregate functions (*first*, *last*, *mean*, *min*, *max*).
 - `DataParser` - parses data arrived in the certain format. For example, CSV or Graphite output data format.
 - `DefaultHandler` - sets default values for columns. Analog of the Kapacitor node of the same name.
 - `FilterHandler` - filters rows with provided conditions. Use `arrow::gandiva` library to create conditions tree.
 - `GroupHandler` - splits record batches into groups with the same values in columns.
 - `MapHandler` - evaluates expressions with present columns as arguments. Use `arrow::gandiva`
   library to create expressions.
 - `SortHandler` - sorts rows by the certain column.
 
### `PeriodNode`

This node type (just like `EvalNode`) takes `PeriodHandler` as strategy to handle record batches belonging to the
certain period of time. It also takes three additional arguments: `range`, `period` and `ts_column_name`. `range` and
`period` arguments defines how long time period will be and how often it will be handled respectively. `ts_column_name`
argument is used to determine which column of record batch contains timestamps. Available `PariodHandlers` are:
 - `JoinHandler` - joins received record batches on the set of columns;
 - `WindowHandler` - concatenates received record batches.
 
## `Producer`

`Producer` provides data to the certain `Node`. There are two types of data producers have been implemented:
 - `TCPProducer` - listens on the certain endpoint for arriving data. It is mostly used to receive data from the
   external data source.
 - `SubscriberProducer` - producer based on ZeroMQ PUB-SUB pattern. Created for transferring data between pipelines.
   As argument it takes `TransportUtils::Subscriber` class containing two ZMQ sockets: subscriber socket and
   synchronize socket. It needs for proper PUB-SUB communicating (for more details see 
   [ZMQ Guide](http://zguide.zeromq.org/page:chapter2#Node-Coordination)).
    
## `Consumer`

As opposite to `Producer` this class consumes data from `Node` and pass it to the next destination. Available types of
consumer:
 - `PrintConsumer` - write record batches to the output stream. `PrintFileConsumer` subclass is more convenient way of
   writing to the file.
 - `TCPConsumer` - writes data to the TCP socket.
 - `PublisherConsumer` - the second part of PUB-SUB pattern.
 
## Helpers

 - As configuring PUB-SUB consumers and producers appears to be unhandy and massive the `NodePipeline::subscribeTo`
   method was implemented. It can be used after nodes of two pipelines have been set to create
   `PublisherConsumer`/`SubscriberProducer` pair for these pipelines without manual creating ZMQ sockets.
 - `utils/` directory is full of useful instruments if you are going to implement some additional functionality by
   yourself.

## Examples

### Parsing Graphite data format example

```cpp
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

#include "consumers/consumers.h"
#include "data_handlers/data_handlers.h"
#include "nodes/nodes.h"
#include "node_pipeline/node_pipeline.h"
#include "producers/producers.h"
#include "utils/parsers/graphite_parser.h"

int main(int argc, char** argv) {
  auto loop = uvw::Loop::getDefault();
  auto zmq_context = std::make_shared<zmq::context_t>(1);

  std::unordered_map<std::string, NodePipeline> pipelines;
  
  std::shared_ptr<Consumer> parse_graphite_consumer = std::make_shared<FilePrintConsumer>("result.txt");

  std::vector<std::string> template_strings{"*.cpu.*.percent.* host.measurement.cpu.type.field"};
  std::shared_ptr<Node> parse_graphite_node = std::make_shared<EvalNode>(
      "parse_graphite_node", std::move(parse_graphite_consumer),
      std::make_shared<DataParser>(std::make_shared<GraphiteParser>(template_strings))
  );

  IPv4Endpoint parse_graphite_producer_endpoint{"127.0.0.1", 4200};
  std::shared_ptr<Producer> parse_graphite_producer = std::make_shared<TCPProducer>(
      parse_graphite_node, parse_graphite_producer_endpoint, loop, true
  );

  pipelines[parse_graphite_node->getName()] = NodePipeline();
  pipelines[parse_graphite_node->getName()].addConsumer(parse_graphite_consumer);
  pipelines[parse_graphite_node->getName()].setNode(parse_graphite_node);
  pipelines[parse_graphite_node->getName()].setProducer(parse_graphite_producer);

  for (auto& pipeline : pipelines) {
    pipeline.second.start();
  }

  loop->run();

  return 0;
}
```

For more complicated examples see `executables\` folder.
