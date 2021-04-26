# ThresholdUDF

## Usage example

Files in this directory represent ThresholdUDF usage.

### Properties

```tickscript
@adjustLevelUDF()
    .watch('longterm')
    .defaultLevel(default_load_level)
    .increaseScaleFactor(2.0)
    .increaseAfter(5s)
    .decreaseTriggerFactor(0.4)
    .decreaseScaleFactor(0.5)
    .decreaseAfter(5s)
    .minLevel(10.0)
    .maxLevel(70.0)
    .as('load_level')
```

* `watch` –- field name used to adjust threshold
* `defaultLevel` –- default threshold value, it is set on all points before 
  the first threshold change
* `increaseScaleFactor` –- threshold scale factor that is used to increase 
  current threshold value if `watch` field value exceeds it for a continuous 
  period defined with `increaseAfter`
* `increaseAfter` – defines how long `watch` field value can exceed current 
  threshold value before the threshold is increased
* `decreaseTriggerFactor` -- relative factor that defines when UDF should try 
  to decrease current threshold value, it is triggered if `watch` field value 
  becomes lesser than `decreaseTriggerFactor` * `currentThreshold`
* `decreaseScaleFactor` –- used to define threshold decrease rule, similarly 
  to `increaseScaleFactor`
* `decreaseAfter` –- used to define threshold decrease rule, similarly
  to `increaseAfter`
* `minLevel` –- minimal threshold value that is allowed
* `maxLevel` –-  maximum threshold value that is allowed
* `as` -- new threshold field name
  
## How to run this example?

### Preparation

Before we start, we need to create a docker volume -- we will use it in 
further steps to share Unix sockets between containers.

```terminal
host$ docker volume create sockets-vol
```

### UDF container

Firstly, you should build a docker image using [Dockerfile](../../Dockerfile) 
from the [root directory](../..). For example, call from the root directory:

```terminal
host$ docker image build --build-arg ALPINE_IMAGE_VERSION=3.12.1 \
        -t qevent_intership_stream_data_processor_image .
```

Next, run the container:

```terminal
host$ docker run -it --rm -v sockets-vol:/var/run/ \
        --name udf-docker \
        -v $PWD:/stream_data_processor \
        qevent_intership_stream_data_processor_image:latest bash
```

After that you should build an UDF executable inside the new container:

```terminal
udf-docker$ cd stream_data_processor
udf-docker$ mkdir build && cd build
udf-docker$ cmake ..
udf-docker$ make threshold_udf
```

Now, start the UDF:

```terminal
udf-docker$ ./bin/sdp-udf --level /var/run/adjustLevelUDF.sock
```

### Kapacitor container

We just started the UDF, let's move to the kapacitor and open a new session. 
We need to run a kapacitor container with everything we need (`recording.srpl`
is your sample recording you want to test):

```terminal
host$ docker run --rm --name kapacitor-container -p 9092:9092 \
        -v $PWD/recording.srpl:/var/lib/kapacitor/replay/recording.srpl \
        -v sockets-vol:/var/run/ \
        -v $PWD/kapacitor.conf:/etc/kapacitor/kapacitor.conf:ro \
        kapacitor
```

Now we can add a task from a new session and test it on the sample data:

```terminal
host$ kapacitor define level_task -tick level.tick
host$ kapacitor enable level_task
host$ kapacitor replay -recording recording -task level_task
```

If needed, you can add a 
[LogNode](https://docs.influxdata.com/kapacitor/v1.5/nodes/log_node/) to the 
tick script to see the result of an UDF and/or enable `--verbose` option for
UDF. **Be careful:** both of this print a lot of logs so I suggest to pipe the 
output to the file.
