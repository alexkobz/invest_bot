import typing as t
from airflow_clickhouse_plugin.hooks.clickhouse import ExecuteReturnT
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from src.utils.path import get_project_root, Path

class ReplicationClickHouseOperator(ClickHouseOperator):

    def __init__(
        self,
        *args,
        filename: str,
        **kwargs
    ) -> None:
        with open(file=f'{Path.joinpath(get_project_root(), "src", "clickhouse", "replication", filename)}.sql') as f:
            sql = f.read().split(';')
        super().__init__(sql=sql, clickhouse_conn_id='clickhouse', *args, **kwargs)

    def execute(self, context: t.Dict[str, t.Any]) -> ExecuteReturnT:
        super().execute(context)
