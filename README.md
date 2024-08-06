# bytewax-clickhouse
ClickHouse Sink for Bytewax

This module is commercially licensed with publicly available source code. Please see the details in LICENSE.md.

## Installation

```bash
pip install bytewax-clickhouse
```

## Usage

ClickHouse Sink requires a PyArrow table, Schema and keys for order and partition.

The Sink is eventually consistent based on the keys.

Add A schema and order by string to your code.

```python
CH_SCHEMA = """
        metric String,
        value Float64,
        ts DateTime,
        """

ORDER_BY = "metric, ts"
```

create batches of data to convert into pyarrow tables.

```python
batched_stream = op.collect(
    "batch_records", metrics, max_size=50, timeout=timedelta(seconds=5)
)
```

Convert the batch to a pyarrow table

```python
PA_SCHEMA = pa.schema([
            ('metric',pa.string()),
            ('value',pa.float64()),
            ('ts',pa.timestamp('us')), # microsecond
        ])


def construct_table(key__batch, pa_schema):
    key, batch = key__batch
    columns = list(zip(*batch))
    arrays = []
    for i, f in enumerate(pa_schema):
        array = pa.array(columns[i], f.type)
        arrays.append(array)
    t = pa.Table.from_arrays(arrays, schema=pa_schema)

    return t

tables = op.map(
    "arrow_table",
    batched_stream,
    lambda batch: construct_table(batch, PA_SCHEMA)
)
```

Use the ClickHouse Sink to write data to ClickHouse

```python
op.output("output_clickhouse", tables, ClickhouseSink("metrics", "admin", "password", database="bytewax", port=8123, schema=CH_SCHEMA, order_by=ORDER_BY))
```