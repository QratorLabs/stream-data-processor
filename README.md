# Stream data processor library (SDP library)

## Kapacitor UDF

### Example "Why you might want to use this library"

Compare the following parts of TICK scripts:

<table>
<tr>
<th>Vanilla TICK script</th>
<th>TICK script with AggregateUDF from SPD library</th>
</tr>
<tr valign="top">
<td>

```tickscript
var cputime_all = stream
 |from()
  .measurement('cpu')
  .groupBy(*)

var cputime_host = cputime_all
 |groupBy('host', 'type')
 |flatten().tolerance(5s)

var cputime_last = cputime_host
 |last('idle').as('idle')
 |eval(
  lambda: "idle", lambda: "nice",
  lambda: "softirq", lambda: "steal",
  lambda: "system", lambda: "user",
  lambda: "wait"
 ).as(
  'idle.last', 'nice.last',
  'softirq.last', 'steal.last',
  'system.last', 'user.last',
  'wait.last'
 )

var cputime_mean_idle = cputime_host
 |mean('idle').as('idle.mean')

var cputime_mean_nice = cputime_host
 |mean('nice').as('nice.mean')

var cputime_mean_softirq = cputime_host
 |mean('softirq').as('softirq.mean')

var cputime_mean_steal = cputime_host
 |mean('steal').as('steal.mean')

var cputime_mean_system = cputime_host
 |mean('system').as('system.mean')

var cputime_mean_user = cputime_host
 |mean('user').as('user.mean')

var cputime_mean_wait = cputime_host
 |mean('wait').as('wait.mean')

var cputime_calc = cputime_mean_idle
 |union(
  cputime_last,
  cputime_mean_nice,
  cputime_mean_softirq,
  cputime_mean_steal,
  cputime_mean_system,
  cputime_mean_user,
  cputime_mean_wait
 )
 |flatten().tolerance(1s)
```

</td>
<td>

```tickscript
var cputime_all = stream
 |from()
  .measurement('cpu')
  .groupBy(*)

var cputime_host = cputime_all
 |groupBy('host', 'type')
 |flatten().tolerance(5s)

var cputime_calc = cputime_host
 @streamAggregateUDF()
  .aggregate('last(idle) as idle.last')
  .aggregate('mean(idle) as idle.mean')
  .aggregate('last(nice) as nice.last')
  .aggregate('mean(nice) as nice.mean')
  .aggregate('last(softirq) as softirq.last')
  .aggregate('mean(softirq) as softirq.mean')
  .aggregate('last(steal) as steal.last')
  .aggregate('mean(steal) as steal.mean')
  .aggregate('last(system) as system.last')
  .aggregate('mean(system) as system.mean')
  .aggregate('last(user) as user.last')
  .aggregate('mean(user) as user.mean')
  .aggregate('last(wait) as wait.last')
  .aggregate('mean(wait) as wait.mean')
  .timeAggregateRule('last')
  .emitTimeout(10s)
```

</td>
</tr>
</table>

The left part of the table represents the possible way you can compute the 
last and the average values of multiple fields using standard TICK syntax. It 
is consisted of several 
[InfluxQLNodes](https://docs.influxdata.com/kapacitor/v1.5/nodes/influx_q_l_node/), 
which also consume pretty big amount of memory.

On the opposite side of the side-by-side comparison you see AggregateUDF 
usage implemented via SDP library. It looks less complicated and requires 
less memory resources.

SDP library provides some UDFs implemented and instruments for creating new 
ones.

### Available UDFs

* [AggregateUDF](examples/aggregate_udf). Convenient instrument for computing 
  aggregate functions like: `mean`, `first`, `last`, `min`, `max`; can apply 
  number of aggregations at the same UDF call; two types of UDF is available 
  for both input data formats: `streamAggragateUDF` and `batchAggregateUDF`.
* [DynamicWindowUDF](examples/dynamic_window_udf). Analogue to Kapacitor's
  [WindowNode](https://docs.influxdata.com/kapacitor/v1.5/nodes/window_node/) 
  with ability to generate windows according to the incoming data. For 
  example, you can specify `period` and `every` properties for every group 
  separately using the 
  [Kapacitor SideloadNode](https://docs.influxdata.com/kapacitor/v1.5/nodes/sideload_node/) 
  and pass this data to the `dynamicWindowUDF` to form windows depending on 
  grouping.
* [ThresholdUDF](examples/threshold_udf). State machine for adjusting 
  thresholds depending on the current value of specified field. May be useful 
  for suppressing flooding alerts when the observable value keeps out of 
  alert range.
  
### UDF documentation

* [UDF User Guide](docs/udf-user-guide.md) -- how to run UDFs
* [UDF implementation](docs/udf-implementation.md) and [building from source
  documentation](docs/build-from-source.md) may be useful for writing your 
  onw UDF using SDP library

## Computation graph

This library also provides instruments for creating distributed asynchronous 
acyclic computation graphs to process continuous stream of arriving data.

## Example

Computation graph consisting of exactly one node which parses Graphite output 
data format:

```c++
#include <memory>

#include <spdlog/spdlog.h>
#include <uvw.hpp>
#include <zmq.hpp>

#include "consumers/consumers.h"
#include "nodes/data_handlers/data_handlers.h"
#include "node_pipeline/node_pipeline.h"
#include "nodes/nodes.h"
#include "nodes/data_handlers/parsers/graphite_parser.h"
#include "producers/producers.h"
#include "utils/utils.h"

namespace sdp = stream_data_processor;

int main(int argc, char** argv) {
  auto loop = uvw::Loop::getDefault();
  auto zmq_context = std::make_shared<zmq::context_t>(1);

  std::unordered_map<std::string, sdp::NodePipeline> pipelines;

  std::shared_ptr<sdp::Consumer> parse_graphite_consumer = std::make_shared<sdp::FilePrintConsumer>("result.txt");

  sdp::GraphiteParser::GraphiteParserOptions parser_options{
      {"*.cpu.*.percent.* host.measurement.cpu.type.field"}
  };

  std::shared_ptr<sdp::Node> parse_graphite_node = std::make_shared<sdp::EvalNode>(
      "parse_graphite_node",
      std::make_shared<sdp::DataParser>(std::make_shared<sdp::GraphiteParser>(parser_options))
  );

  sdp::transport_utils::IPv4Endpoint parse_graphite_producer_endpoint{"127.0.0.1", 4200};
  std::shared_ptr<sdp::Producer> parse_graphite_producer = std::make_shared<sdp::TCPProducer>(
      parse_graphite_node, parse_graphite_producer_endpoint, loop.get(), true
  );

  pipelines[parse_graphite_node->getName()] = sdp::NodePipeline();
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

## Computation graph references

* [Computation graphs documentation](docs/computation-graph.md) -- more 
  information about computation graphs' architecture and how to write them.
* [More complicated example](examples/tick_script_adapted.cpp) representing 
  [cpu.tick](examples/aggregate_udf/cpu.tick) analogue written in C++ using 
  Computation graphs from SDP library.
