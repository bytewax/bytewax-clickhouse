import logging
from typing import List, Optional, TypeVar

from bytewax.outputs import DynamicSink, StatelessSinkPartition
from clickhouse_connect import get_client
from pyarrow import Table, concat_tables
from typing_extensions import override

K = TypeVar("K")
"""Type of key in Kafka message."""

V = TypeVar("V")
"""Type of value in a Kafka message."""

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class _ClickHousePartition(StatelessSinkPartition):
    def __init__(
        self,
        table_name: str,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
    ):
        self.table_name = table_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.client = get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
        )

    @override
    def write_batch(self, batch: List[Table]) -> None:
        arrow_table = concat_tables(batch)
        self.client.insert_arrow(
            f"{self.database}.{self.table_name}",
            arrow_table,
            settings={"buffer_size": 0},
        )


class ClickHouseSink(DynamicSink):
    def __init__(
        self,
        table_name: str,
        username: str,
        password: str,
        host: str = "localhost",
        port: int = 8123,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        order_by: str = "",
    ):
        self.table_name = table_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.schema = schema

        # init client
        if not self.database:
            logger.warning("database not set, using 'default'")
            self.database = "default"
        client = get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
        )

        # Check if the table exists
        table_exists_query = f"EXISTS {self.database}.{self.table_name}"
        table_exists = client.command(table_exists_query)
        if not table_exists:
            logger.info(
                f"""Table '{self.table_name}' does not exist.
                        Attempting to create with provided schema"""
            )
            if schema:
                # Create the table with ReplacingMergeTree
                create_table_query = f"""
                CREATE TABLE {database}.{table_name} (
                    {self.schema}
                ) ENGINE = ReplacingMergeTree()
                ORDER BY tuple({order_by});
                """
                client.command(create_table_query)
                logger.info(f"Table '{table_name}' created successfully.")
            else:
                msg = """Bad Schema. Can't complete execution without schema of format
                        column1 UInt32,
                        column2 String,
                        column3 Date"""
                raise ValueError(msg)
        else:
            logger.info(f"Table '{self.table_name}' exists.")

            # Check the MergeTree type
            mergetree_type_query = f"""SELECT engine FROM system.tables
                    WHERE database = '{self.database}' AND name = '{self.table_name}'"""
            mergetree_type = client.command(mergetree_type_query)
            logger.info(f"MergeTree type of the table '{table_name}': {mergetree_type}")

            if "ReplacingMergeTree" not in mergetree_type:
                logger.warning(
                    f"""Table '{table_name}' is not using ReplacingMergeTree.
                    Consider modifying the table to avoid performance degredation
                    and/or duplicates on restart"""
                )

            # Get the table schema
            schema_query = f"""
            SELECT name, type FROM system.columns
            WHERE database = '{self.database}' AND table = '{self.table_name}'
            """
            schema_result = client.query(schema_query)
            columns = schema_result.result_rows
            logger.info(f"Schema of the table '{self.table_name}':")
            for column in columns:
                logger.info(f"Column: {column[0]}, Type: {column[1]}")

        client.close()

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _ClickHousePartition:
        return _ClickHousePartition(
            self.table_name,
            self.host,
            self.port,
            self.username,
            self.password,
            self.database,
        )
