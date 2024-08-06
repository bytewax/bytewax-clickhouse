from datetime import timedelta, datetime 

from bytewax import operators as op
from bytewax.connectors.demo import RandomMetricSource
from bytewax.dataflow import Dataflow
import pyarrow as pa

from clickhouse_connector import ClickhouseSink

CH_SCHEMA = """
        metric String,
        value Float64,
        ts DateTime,
        """

ORDER_BY = "metric, ts"

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


flow = Dataflow("test_ch")

# Build a sample stream of metrics
metrica = op.input("inp_a", flow, RandomMetricSource("a_metric"))
metricb = op.input("inp_b", flow, RandomMetricSource("b_metric"))
metricc = op.input("inp_c", flow, RandomMetricSource("c_metric"))
metrics = op.merge("merge", metrica, metricb, metricc)
metrics = op.map("add_time", metrics, lambda x: x + tuple([datetime.now()]))
metrics = op.map("add_key", metrics, lambda x: ("All", x))
# op.inspect("metrics", metrics)

batched_stream = op.collect(
    "batch_records", metrics, max_size=50, timeout=timedelta(seconds=5)
)
tables = op.map(
    "arrow_table",
    batched_stream,
    lambda batch: construct_table(batch, PA_SCHEMA)
)
op.inspect("message_stat_strings", tables)
op.output("output_clickhouse", tables, ClickhouseSink("metrics", "admin", "password", database="bytewax", port=8123, schema=CH_SCHEMA, order_by=ORDER_BY))