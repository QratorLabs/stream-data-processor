# YAML configuration proposal

## Общие принципы работы библиотеки

* Граф вычислений разделяется на пайплайны. В каждом пайплайне 3 составляющих: 
`Producer`s (источники данных), `Node` (обработчик данных) и `Consumer`s 
(потребители обработанных данных).
* Пайплайн `A` может соединяться с пайплайном `B` передачи своего результата 
обработки на вход пайплайну `B` с помощью механизма `Consumer`s и `Producer`s.
* Одни из реализаций `Consumer` и `Producer` -- это `PublisherConsumer` и 
`SubscriberConsumer` соответственно. Они работают, используя [соответствующий 
паттерн передачи данных из библиотеки 
ZeroMQ](https://zguide.zeromq.org/docs/chapter1/#Getting-the-Message-Out).
* `Node`s бывают двух типов: `EvalNode` и `PeriodNode` (возможно, в будущем 
могут появиться другие). Каждый из них принимает свой тип обработчика
(`DataHandler` и `PeriodNode` соответственно) и может принимать специальные
параметры. Так, например, `PeriodNode` принимает длину периода и интервал
между началами соседних периодов.
* Хэндлеры, занимающиеся обработкой **промежуточных** данных, в качестве стратегии
принимают `RecordBatchHandler`, каждый тип которого также принимает ряд
параметров.
* Одна из реализаций `RecordBatchHandler` -- это `PipelineHandler`, содержащий
вектор из других хэндлеров. Такой объект был создан для оптимизации выполнения
последовательных обработок за счет избежания лишней сериализации и передачи 
данных. Это также может быть использовано в YAML-синтаксисе конфигурационного 
файла для избежания дополнительной вложенности.
* Некоторые реализации `RecordBatchHandler` (например, `MapHandler`) активно 
используют модуль 
[Gandiva](https://github.com/apache/arrow/tree/master/cpp/src/gandiva) из 
библиотеки `Apache Arrow`: в качестве параметров они могут принимать объекты 
этой библиотеки, потому что пока не поддерживают самостоятельный парсинг 
лямбд.

## Возможный синтаксис конфигурационного YAML-файла

### Описание ноды

```yaml
&example_node example_node:
  # producers: where data come from
  producers:
    # ...

  # node_options: node's type, data handler, etc.
  node_options:
    # ...
```

### `producers`

На данный момент реализовано 2 источника данных для ноды: `SubscriberProducer`
и `TCPProducer`. Первый предназначен и более предпочтителен для передачи 
данных между нодами. Второй обычно используется для внешних источников и, 
как правило, обработчиком данных у ноды с `Producer` такого типа будет 
являться `DataParser`.

Соответственно, для описания `producer` типа `node` достаточно указать имя 
ноды-источника:

```yaml
&producer_node producer_node: # ...

&consumer_node consumer_node:
  producers:
    - type: node 
      node: *producer_node
```

Для `producer` типа `tcp` нужно указать `host` и `port` на которых он будет 
слушать входящие подключения:

```yaml
&tcp_node tcp_node:
  producers:
    - type: tcp
      host: "127.0.0.1"
      port: 4200
```

### `node_options`

На данный момент есть 2 типа нод: `eval` и `period`. Для `period` ноды 
необходимо дополнительно указать продолжительность временного окна `range`, 
период между началами соседних окон `period`.

Помимо прочего обязательным параметром является `data_handler` -- обработчик 
данных.

```yaml
&eval_node eval_node:
  node_options:
    type: eval
    data_handler: # ...

&period_node period_node:
  node_options:
    type: period
    range: 10s
    period: 2s
    data_handler: # ...
```

### `data_handler`

На данный момент возможны 2 типа обработчиков: `parser` и `pipeline`.

### `parser`

Доступен только для `node` типа `eval`. Занимается преобразованием
данных произвольного типа в промежуточный колоночный формат, с которым 
работают все остальные обработчики. Обычно используется в связке с `producer` 
типа `node` чтобы принимать данные из внешних источников в определенном 
формате и подгатавливать их к работе. В качестве параметра `format` принимает 
формат приходящих данных. Поддерживаемые в данный момент форматы: `csv`, 
`graphite`.

```yaml
&parser_node parser_node:
  producers:
    - type: tcp
      host: "127.0.0.1"
      port: 4200
  node_options:
    type: eval
    data_handler:
      type: parser
```

* `csv` parser:

В данный момент поддерживается формат `CSV`, в котором присутствуют названия 
колонок.

```yaml
&csv_parser_node csv_parser_node:
  producers:
    - type: tcp
      host: "127.0.0.1"
      port: 4200
  node_options:
    type: eval
    data_handler:
      type: parser
      format: csv
```

* `graphite` parser:

```yaml
&graphite_parser_node graphite_parser_node:
  producers:
    - type: tcp
      host: "127.0.0.1"
      port: 4200
  node_options:
    type: eval
    data_handler:
      type: parser
      format: graphite
      templates: [
        "*.*.* region.region.measurement",
        "*.*.*.* region.region.host.measurement"
      ]
      separator: "_"
```

**Параметры**:
  - `templates`: [шаблоны для парсинга формата `Graphite`](https://docs.influxdata.com/influxdb/v1.8/supported_protocols/graphite/)
  - `separator`: опциональный параметр. Разделитель, по которому названия 
    столбцов будут конкатенироваться. По умолчанию используется `"."`.

### `pipeline`

Обработчкик данных в колоночном формате, основной инструмент 
библиотеки. Имеет параметр `handlers`, значение которого -- список 
последовательных хэндлеров.

```yaml
&pipeline_node pipeline_node:
  node_options:
    data_handler:
      type: pipeline
      handlers: # ...
```

### `handlers`

* `aggregate`:

```yaml
&aggregate_node aggregate_node:
  node_options:
    data_handler:
      type: pipeline
      handlers:
        - type: aggregate
          aggregates:
            - column_name: "field_1"
              aggregate_cases: [
                [ last, "field_1_last" ],
                [ mean, "field_1_mean" ]
              ]
            - column_name: "field_2"
              aggregate_cases: [
                [ last, "field_2_last" ],
                [ mean, "field_2_mean" ]
              ]
          grouping_columns: [ "measurement" ]
          result_time_column_rule: last
```

**Параметры**:
  - `aggregates`: определяет колонки и функции для агрегации, а также названия
    конечных колонок;
  - `grouping_columns`: колонки, по которым будет происходить группировка: 
    элементы разных групп агрегируются по отдельности;
  - `result_time_column_rule`: опциональный параметр. Определяет правило для 
    агрегации колонки со временем. По умолчанию используется функция `last`.
    
* `default`:

```yaml
&default_node default_node:
  node_options:
    data_handler:
      type: pipeline
      handlers:
        - type: default
          int_columns: [
            ["int_field_1", 42],
            ["int_field_2", -10]
          ]
          double_columns: # ...
          string_columns: # ...
          bool_columns: # ...
```

**Параметры**: `int_columns`, `double_columns`, `string_columns`, 
`bool_columns` -- названия и значения по умолчанию для целочисленных, 
вещественных, строковых и логических колонок соотвественно.

* `group`:

```yaml
&group_node group_node:
  node_options:
    data_handler:
      type: pipeline
      handlers:
        - type: group
          grouping_columns: [ "measurement, tag_1, tag_2" ]
```

**Параметры**: `grouping_columns` -- набор названий колонок по которым будет
происходить группировка. Гарантируется, что из этого обработчика данные будут 
выдаваться строго по группам.

* `join`:

Как правило использвется в нодах с несколькими `producer`s.

```yaml
&join_node join_node:
  node_options:
    data_handler:
      type: pipeline
      handlers:
        - type: join
          join_on_columns: [ "measurement", "tag_name" ]
          tolerance: 2s
```

**Параметры**:
  - `join_on_columns`: названия колонок по которым данные будут объединятся
    (при этом данные всегда объединяются по времени);
  - `tolerance`: опциональный параметр. Если присутствует, записи, временные 
    метки которых отличаются не более, чем на `tolerance`, считаются 
    одинаковыми.
    
* `sort`:

```yaml
&sort_node sort_node:
  node_options:
    data_handler:
      type: pipeline
      handlers:
        - type: sort
          sort_by_columns: [ "measurement", "tag_name" ]
```

**Параметры**: `sort_by_columns` -- набор названий колонок по которым будет
происходить сортировка. Каждая следующая сортировка является устойчивой: 
происходит в рамках уже отсортированных групп.

* `window`:

Объединяет пришедшие данные в одну группу. Как правило используется в связке с
`period_node`.

```yaml
&window_node window_node:
  node_options:
    data_handler:
      type: pipeline
      handlers:
        - type: window
```
