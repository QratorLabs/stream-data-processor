# How to run this example?

## `docker-compose`

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

# Explanation

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
