# DynamicWindowUDF

## Usage example

Files in this directory represent DynamicWindowUDF usage in bundle with 
[Kapacitor SideloadNode](https://docs.influxdata.com/kapacitor/v1.5/nodes/sideload_node/)

### Properties

```tickscript
@dynamicWindowUDF()
    .periodField('period')
    .periodTimeUnit('m')
    .everyField('every')
    .everyTimeUnit('m')
    .fillPeriod()
    .defaultPeriod(default_period)
    .defaultEvery(default_every)
    .emitTimeout(10s)
```

* `periodField` –- `period` field name
* `periodTimeUnit` –- `period` values time unit to interpret them correctly: ns 
  – nanoseconds, u – microseconds, ms – milliseconds, s – seconds, m – 
  minutes, h – hours, d – days, w – weeks
* `everyField` –- `every` field name
* `everyTimeUnit` –- `every` values time unit, similarly to `periodTimeUnit`
* `fillPeriod` –- optional property. Defines if UDF should wait until the first 
  window is fully filled (Kapacitor WindowNode's `fillPeriod` property 
  analogue)
* `defaultPeriod` –- `period` default value
* `defaultEvery` -– `every` default value
* `emitTimeout` -–  UDF accumulates several points before processing. 
  `emitTimeout` property defines timeout between two sequential processing 
  moments

## How to run this example?

### `docker-compose`

Just run the following in the current directory:

```terminal
$ docker-compose up
```

Now you can use `kapacitor` commands right from your host system. For example,
you can run [dynamic_window.tick](dynamic_window.tick) and watch the results:

```terminal
$ kapacitor define dynamic_window_task -tick dynamic_window.tick
$ kapacitor enable dynamic_window_task
$ kapacitor watch dynamic_window_task
```

After that you will able to see points coming from UDF.

To remove intermediate build container, call:

```terminal
$ docker image prune --filter "label=stage=builder" --filter "label=project=dynamic_window_udf_example" --force
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
