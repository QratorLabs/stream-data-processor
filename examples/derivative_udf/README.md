# DerivativeUDF

This UDF calculates n-th derivatives of incoming time series data.

## Usage example

Files in this directory represent DerivativeUDF usage.

## Properties

```tickscript
@derivativeUDF()
    .derivative('idle').as('idle_der')
    .derivative('idle').order(2).as('idle_der_2')
    .unit(10s).neighbourhood(20s).noWait()
    .emitTimeout(10s)
```

* `derivative` -- field name which values are used to calculate derivatives.
* `order` (optional) -- derivative's order. **Default `order` value** is 1.
* `as` -–  result field name.
* `unit` (optional) -- defines an unit time interval used to calculate 
  derivative. **Default `unit` value** is `1s`. Value of this property is 
  applied to all derivative cases in the UDF call.
* `neighbourhood` (optional) -- defines a time interval values from which will
  be used to calculate derivative with 
  [finite difference](https://en.wikipedia.org/wiki/Finite_difference) 
  approach. **Default `neighbourhood` value** is equal to `unit` property 
  value. Value of this property is applied to all derivative cases in the 
  UDF call. **Note**: if `noWait` is not used this property affects a time
  delay between sending a point to the UDF and retrieving result from it.
* `noWait` -- makes UDF to use backward difference approach so result is 
  returned almost immediately (in fact, at most after `emitTimeout`) after 
  receiving the last point.
* `emitTimeout` -–  UDF accumulates several points before processing.
  `emitTimeout` property defines timeout between two sequential processing
  moments.

## How to run this example?

### `docker-compose`

Just run the following in the current directory:

```terminal
$ docker-compose up
```

Now you can use `kapacitor` commands right from your host system. For example,
you can run [derivative.tick](derivative.tick) and watch the results:

```terminal
$ kapacitor define der_task -tick derivative.tick
$ kapacitor enable der_task
$ kapacitor watch der_task
```

After that you will able to see points coming from UDF.

To remove intermediate build container, call:

```terminal
$ docker image prune --filter "label=stage=builder" --filter "label=project=derivative_udf_example" --force
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
