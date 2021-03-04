# AggregateUDF

This UDF aggregates incoming points **by groups** or **by batches** 
(depending on input data type).

## Usage example

Files in this directory represent AggregateUDF usage.

## Properties

### `streamAggregateUDF`

This UDF can be used with stream input edge only.

```tickscript
@streamAggregateUDF()
    .aggregate('last(idle) as idle.last')
    .aggregate('mean(idle) as idle.mean')
    .aggregate('last(interrupt) as interrupt.last')
    .aggregate('mean(interrupt) as interrupt.mean')
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
    .tolerance(1s)
```

* `aggregate` -- defines one aggregation using syntax: 
  `<aggregateFunction>(<fieldName>) as <resultFieldName>`. Currently available 
  aggregate functions are:
    * `first`
    * `last`
    * `min`
    * `max`
    * `mean`
* `timeAggregateRule` -- defines aggregate rule for result timestamp. It 
  takes one of available aggregate functions as an argument
* `emitTimeout` -–  UDF accumulates several points before processing.
  `emitTimeout` property defines timeout between two sequential processing
  moments
* `tolerance` (optional) -- defines time interval between the first and the 
  last points in aggregating batch. Default `tolerance` value is `0s` which
  means that points aggregated together have equal timestamps.
  
### `batchAggregateUDF`

This UDF can be used with batch input edge only. Its properties do not contain
`emitTimeout` and `tolerance` properties as this UDF aggregate every batch it 
receives separately.

```tickscript
@batchAggregateUDF()
    .aggregate('last(idle) as idle.last')
    .aggregate('mean(idle) as idle.mean')
    .aggregate('last(interrupt) as interrupt.last')
    .aggregate('mean(interrupt) as interrupt.mean')
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
```

## How to run this example?

### `docker-compose`

Just run the following in the current directory:

```terminal
$ docker-compose up
```

Now you can use `kapacitor` commands right from your host system. For example,
you can run [cpu.tick](cpu.tick) and watch the results:

```terminal
$ kapacitor define cpu_task -tick cpu.tick
$ kapacitor enable cpu_task
$ kapacitor watch cpu_task
```

After that you will able to see points coming from UDF.

To remove intermediate build container, call:

```terminal
$ docker image prune --filter "label=stage=builder" --filter "label=project=aggregate_udf_example" --force
```

### Explanation

This example uses number of Docker containers:

* kapacitor-udf -- container with UDF. Uses Docker image built from
[Dockerfile](../../Dockerfile).
* [collectd](https://registry.hub.docker.com/r/fr3nd/collectd) -- metrics
producer. Sends them to InfluxDB. In theory, can be replaced.
* [indluxdb](https://registry.hub.docker.com/_/influxdb) -- InfluxDB container.
Parses [Graphite data format](https://docs.influxdata.com/influxdb/v1.7/supported_protocols/graphite/#)
coming from collectd. Provides metrics for Kapacitor.
* [kapacitor](https://registry.hub.docker.com/_/kapacitor) -- Kapacitor
container. Subscribes to InfluxDB to receive metrics, processes them with
[TICK scripts](https://docs.influxdata.com/kapacitor/v1.5/tick/syntax/#),
communicates with UDF.
