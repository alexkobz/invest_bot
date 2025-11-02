from airflow.operators.bash import BashOperator


class DbtOperator(BashOperator):
    def __init__(
        self,
        model: str = None,
        snapshot: str = None,
        **kwargs,
    ):
        if model:
            bash_command = f"cd /opt/airflow/dbt && dbt run --select {model}"
        elif snapshot:
            bash_command = f"cd /opt/airflow/dbt && dbt snapshot --select {snapshot}"
        else:
            raise ValueError("Model or snapshot must be specified")

        # обязательно передаем bash_command в super
        super().__init__(bash_command=bash_command, **kwargs)

    def execute(self, context):
        return super().execute(context)
