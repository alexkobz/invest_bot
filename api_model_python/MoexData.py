from __future__ import annotations

import os
import pandas as pd
from airflow.exceptions import AirflowSkipException
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text

from api_model_python.plugins.clients.Moex.MoexAPI import Request, Prices, Boards, SecuritiesTrading
from api_model_python.plugins.clients.Moex.iss_client import (
    Config,
    MicexAuth,
    MicexISSClientPrices,
    MicexISSClientBoards,
    MicexISSClientSecurities)
from api_model_python.plugins.path import get_project_root, Path

from exceptions.MoexAuthenticationError import MoexAuthenticationError
from logs.Logger import Logger


logger = Logger()


class MoexData:

    _instance = None

    def __init__(self):
        if self._instance.__initialized:
            return
        self._instance.__initialized = True
        self.my_config = None
        self.my_auth = None
        self.engine = None
        self.init_iss()

    @staticmethod
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.__initialized = False
        return cls._instance

    def init_iss(self):
        env_path: Path = Path.joinpath(get_project_root(), '.env')
        load_dotenv(env_path)
        self.my_config: Config = Config(
            user=os.environ["EMAIL_LOGIN"],
            password=os.environ["EMAIL_PASSWORD"],
            proxy_url='')
        self.my_auth: MicexAuth = MicexAuth(self.my_config)
        if self.my_auth.is_real_time():
            DATABASE_URI: str = (
                f"postgresql://"
                f"{os.environ['POSTGRES_USER']}:"
                f"{os.environ['POSTGRES_PASSWORD']}@"
                f"{os.environ['POSTGRES_HOST']}:"
                f"{os.environ['POSTGRES_PORT']}/"
                f"{os.environ['POSTGRES_DATABASE']}")
            self.engine = create_engine(DATABASE_URI)
        else:
            logger.info(f"{str(MoexAuthenticationError)}")
            raise MoexAuthenticationError()

    def finish_get_data(self, df: pd.DataFrame, method: Request.__annotations__):
        if df.empty:
            raise AirflowSkipException("Skipping this task as DataFrame is empty")
        df.columns = df.columns.str.lstrip('@')
        df.columns = df.columns.str.lower()
        self.engine.execute(
            sa_text(f'''TRUNCATE TABLE {method.table_name}''').execution_options(autocommit=True))
        df.to_sql(name=method.table_name, con=self.engine, if_exists='append', index=False)
        logger.info(f"{method.table_name} downloaded successfully")


    def get_boards(self):
        iss = MicexISSClientBoards(self.my_config, self.my_auth)
        df: pd.DataFrame = iss.get_data()
        self.finish_get_data(df, Boards)

    def get_securities_trading(self):
        iss = MicexISSClientSecurities(self.my_config, self.my_auth)
        df: pd.DataFrame = iss.get_data()
        self.finish_get_data(df, SecuritiesTrading)

    def get_prices(self):
        boardids = (
            pd.read_sql(
                """
                SELECT DISTINCT boardid
                FROM public_marts.dim_moex_boards
                WHERE is_traded = true
                """
                , self.engine
            )['boardid']
            .to_list()
        )
        iss = MicexISSClientPrices(self.my_config, self.my_auth)
        moex_prices_df: pd.DataFrame = pd.DataFrame()
        for boardid in boardids:
            df: pd.DataFrame = iss.get_data(boardid)
            moex_prices_df = pd.concat([moex_prices_df, df], axis=0)
        self.finish_get_data(moex_prices_df, Prices)
