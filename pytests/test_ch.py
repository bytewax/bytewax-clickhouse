from datetime import timedelta, datetime 

from bytewax import operators as op
from bytewax.connectors.demo import RandomMetricSource
from bytewax.dataflow import Dataflow
import pyarrow as pa

from bytewax.connectors.clickhouse import operators as chop

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


flow = Dataflow("test_ch")

# Build a sample stream of metrics
metrica = op.input("inp_a", flow, RandomMetricSource("a_metric"))
metricb = op.input("inp_b", flow, RandomMetricSource("b_metric"))
metricc = op.input("inp_c", flow, RandomMetricSource("c_metric"))
metrics = op.merge("merge", metrica, metricb, metricc)
metrics = op.map("add_time", metrics, lambda x: x + tuple([datetime.now()]))
metrics = op.map("add_key", metrics, lambda x: ("All", x))
op.inspect("metrics", metrics)


chop.output("output_clickhouse", metrics, "metrics", "admin", "password", database="bytewax", port=8123, schema=CH_SCHEMA, order_by=ORDER_BY, pa_schema=PA_SCHEMA)