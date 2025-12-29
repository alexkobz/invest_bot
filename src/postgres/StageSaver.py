from typing import Callable

import pandas as pd
from pandas import DataFrame
from functools import wraps
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from src.postgres.engine import get_engine


class StageSaver:
    def __init__(self, table_name: str, schema: str = 'public'):
        if table_name is None:
            raise ValueError("Table name cannot be None")
        self.table_name = table_name
        self.schema = schema
        self.engine: Engine = get_engine(schema=schema)
        self.inspector = inspect(self.engine)

    def before_save(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def after_save(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def save_stg(
        self,
        df: pd.DataFrame,
    ) -> bool:
        if df is None or df.empty:
            return False
        if self.inspector.has_table(self.table_name):
            self.engine.execute(f'''TRUNCATE TABLE "{self.table_name}"''', autocommit=True)
        df.to_sql(
            name=self.table_name,
            con=self.engine,
            if_exists='append',
            index=False
        )
        return True

    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> DataFrame:
            df = func(*args, **kwargs)
            df = self.before_save(df)
            self.save_stg(df)
            df = self.after_save(df)
            return df
        return wrapper
