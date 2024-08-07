"""Operators for the ClickHouse sink.

It's suggested to import operators like this:

```{testcode}
from bytewax.connectors.clickhouse import operators as chop
```

And then you can use the operators like this:

```{testcode}
from bytewax.dataflow import Dataflow

flow = Dataflow("kafka-in-out")
input = kop.input("kafka_inp", flow, brokers=[...], topics=[...])
chop.output("ch-out", input, )
```
"""
from datetime import timedelta
from typing import Tuple, Any, List, Optional
from typing_extensions import TypeAlias

import bytewax.operators as op
from bytewax.dataflow import Stream, operator

from bytewax.connectors.clickhouse import ClickHouseSink, V

import pyarrow as pa


KeyedStream: TypeAlias = Stream[Tuple[str, V]]
"""A {py:obj}`~bytewax.dataflow.Stream` of `(key, value)` 2-tuples."""

@operator
def _to_sink(
    step_id: str, up: KeyedStream[V], timeout: timedelta, max_size: int,
    pa_schema: pa.Schema
) -> KeyedStream[List[V]]:
    """Convert records to PyArrow Table"""

    def shim_mapper(
        key__batch: Tuple, pa_schema
    ) -> pa.Table:
        key, batch = key__batch
        columns = list(zip(*batch))
        arrays = []
        for i, f in enumerate(pa_schema):
            array = pa.array(columns[i], f.type)
            arrays.append(array)
        t = pa.Table.from_arrays(arrays, schema=pa_schema)

        return t

    return op.collect().then(op.map, "map", shim_mapper)


@operator
def output(
    step_id: str,
    up: KeyedStream[V],
    table_name: str,
    username: str,
    password: str,
    host: str = "localhost",
    port: int = 8123,
    database: Optional[str] = None,
    ch_schema: Optional[str] = None,
    order_by: str = '',
    pa_schema: Optional[pa.Schema] = None
) -> None:
    """Produce to ClickHouse as an output sink.

    Uses Arrow format, must be arrow serializiable.

    Default partition routing is used.

    Workers are the unit of parallelism.

    Can support at-least-once processing depending on
    the MergeTree used for downstream queries.

    :arg step_id: Unique ID.

    :arg up: Stream of records. Key must be a `String`
        and value must be serializable into an arrow table.

    :arg table_name: Table name for the writes.

    :arg username: database username, user must have
        correct permissions.

    :arg password:

    :arg host: host name, defaults to "localhost".

    :arg port: port name, defaults to 8123.

    :arg database: optional database name. If omitted
        this will use the default database.

    :arg ch_schema: schema string of format
                        ```column1 UInt32,
                        column2 String,
                        column3 Date```,

    :arg order_by: order by string that determines the sort of
        the table for deduplication. Should be of format:
        `metric, ts`

    :arg pa_schema: Arrow schema.

    """
    return _to_sink("to_sink", up).then(
        op.output, "kafka_output", ClickHouseSink(table_name, username,
                                                  password, host, port,
                                                  database, ch_schema,
                                                  order_by, pa_schema)
    )
