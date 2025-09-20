from __future__ import annotations

import os
import pandas as pd
import xmltodict
from airflow.exceptions import AirflowSkipException
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text
import json
import xml.etree.ElementTree as ET

import pandas as pd
import xmltodict
from typing_extensions import override
from urllib3 import HTTPResponse

from .MoexDocs import *
from .iss_client import MicexAuth, Config
from src.utils.path import Path, get_project_root
from src.exceptions.MoexAuthenticationError import MoexAuthenticationError
from logs.Logger import Logger

SCHEMA = 'moex'

logger = Logger()
class Moex():

    _instance = None

    def __init__(self):
        if self._instance.__initialized:
            return
        self._instance.__initialized = True
        self.config = None
        self.auth = None
        self.engine = None
        self._init_iss()

    @staticmethod
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.__initialized = False
        return cls._instance

    def _init_iss(self):
        env_path: Path = Path.joinpath(get_project_root(), '.env')
        load_dotenv(env_path)
        self.config: Config = Config(
            user=os.environ["EMAIL_LOGIN"],
            password=os.environ["EMAIL_PASSWORD"],
            proxy_url='')
        self.auth: MicexAuth = MicexAuth(self.config)
        if self.auth.is_real_time():
            DATABASE_URI: str = (
                f"postgresql://"
                f"{os.environ['POSTGRES_USER']}:"
                f"{os.environ['POSTGRES_PASSWORD']}@"
                f"{os.environ['POSTGRES_HOST']}:"
                f"{os.environ['POSTGRES_PORT']}/"
                f"{os.environ['POSTGRES_DATABASE']}")
            self.engine = create_engine(DATABASE_URI)
        else:
            logger.exception(f"{str(MoexAuthenticationError)}")
            raise MoexAuthenticationError()
        logger.info(f"Moex auth is successfully initialized")
        if self.config.proxy_url:
            self.opener = urllib.request.build_opener(
                urllib.request.ProxyHandler({"http": self.config.proxy_url}),
                urllib.request.HTTPCookieProcessor(self.auth.cookie_jar),
                urllib.request.HTTPHandler(debuglevel=self.config.debug_level),
            )
        else:
            self.opener = urllib.request.build_opener(
                urllib.request.HTTPCookieProcessor(self.auth.cookie_jar),
                urllib.request.HTTPHandler(debuglevel=self.config.debug_level),
            )
        urllib.request.install_opener(self.opener)

    def _send_request(self, url: str) -> str:
        response = self.opener.open(url)
        return response.read().decode('utf-8')

    def _finish_get_data(self, df: pd.DataFrame, table_name: str) -> bool:
        if df.empty:
            raise AirflowSkipException("Skipping this task as DataFrame is empty")
        try:
            df.columns = df.columns.str.lstrip('@')
            df.columns = df.columns.str.lower()
            df.to_sql(
                name=table_name,
                schema=SCHEMA,
                con=self.engine,
                if_exists='replace',
                index=False)
            logger.info(f"{table_name} downloaded successfully")
            return True
        except Exception as e:
            logger.exception(f"{table_name} failed to download due to\n{e}")
            return False

    def getStockSharesSecurities(self) -> bool:
        url = StockSharesSecurities().url()
        res = self._send_request(url)
        res = xmltodict.parse(res)
        data = res['document']['data']
        securities = data[0]
        securities_df = pd.DataFrame(securities['rows']['row'])
        is_success_securities: bool = self._finish_get_data(securities_df, 'StockSharesSecurities')
        marketdata = data[1]
        marketdata_df = pd.DataFrame(marketdata['rows']['row'])
        is_success_marketdata: bool = self._finish_get_data(marketdata_df, 'StockSharesSecuritiesMarketData')
        return is_success_securities & is_success_marketdata

    def getStockSharesBoards(self) -> bool:
        url = StockSharesBoards().url()
        boards = self._send_request(url)
        data = xmltodict.parse(boards)
        df = pd.DataFrame(data['document']['data']['rows']['row'])
        is_success: bool = self._finish_get_data(df, 'StockSharesBoards')
        return is_success

    def getEngineStock(self) -> bool:
        url = EngineStock().url()
        engine_stock = self._send_request(url)
        return engine_stock


# class MicexISSClientBoards(MicexISSClient):
#
#     @override
#     def get_data(self) -> pd.DataFrame:
#         """Get and parse historical data."""
#         url = Boards.url
#         response = self.opener.open(url)
#         boards = response.read().decode('utf-8')
#         data = xmltodict.parse(boards)
#         result = pd.DataFrame(data['document']['data']['rows']['row'])
#         return result
#
#
# class MicexISSClientPrices(MicexISSClient):
#
#     @override
#     def get_data(self, board: str) -> pd.DataFrame:
#         """Get and parse historical data."""
#         url = Prices.url % {
#             'engine': Prices.engine,
#             'market': Prices.market,
#             'board': board,
#             'date': Prices.date,
#         }
#
#         start = 0
#         result = pd.DataFrame()
#         while True:
#             res = self.opener.open(f"{url}&start={start}")
#             jres = json.load(res)
#
#             jhist = jres['history']
#             jdata = jhist['data']
#             jcols = jhist['columns']
#             if not jdata:
#                 break
#
#             chunk = pd.DataFrame(data=jdata, columns=jcols)
#             result = pd.concat([result, chunk], axis=0)
#             start += len(jdata)
#         return result
#
#
# class MicexISSClientShares(MicexISSClient):
#
#     @override
#     def get_data(self) -> pd.DataFrame:
#         """Get and parse data of shares."""
#         url = 'http://iss.moex.com/iss/engines/stock/markets/shares/securities'
#         response: HTTPResponse = self.opener.open(url)
#         result: str = response.read().decode('utf-8')
#         root = ET.fromstring(result)
#         rows = [row.attrib for row in root.findall(".//row")]
#         df = pd.DataFrame(rows)
#         return df
#
#
# def del_null(num):
#     """Replace null with zero."""
#     return 0 if num is None else num


# class MoexData:
#
#     _instance = None
#
#     def __init__(self):
#         if self._instance.__initialized:
#             return
#         self._instance.__initialized = True
#         self.my_config = None
#         self.my_auth = None
#         self.engine = None
#         self.init_iss()
#
#     @staticmethod
#     def __new__(cls, *args, **kwargs):
#         if cls._instance is None:
#             cls._instance = super().__new__(cls)
#             cls._instance.__initialized = False
#         return cls._instance
#
#     def init_iss(self):
#         env_path: Path = Path.joinpath(get_project_root(), '.env')
#         load_dotenv(env_path)
#         self.my_config: Config = Config(
#             user=os.environ["EMAIL_LOGIN"],
#             password=os.environ["EMAIL_PASSWORD"],
#             proxy_url='')
#         self.my_auth: MicexAuth = MicexAuth(self.my_config)
#         if self.my_auth.is_real_time():
#             DATABASE_URI: str = (
#                 f"postgresql://"
#                 f"{os.environ['POSTGRES_USER']}:"
#                 f"{os.environ['POSTGRES_PASSWORD']}@"
#                 f"{os.environ['POSTGRES_HOST']}:"
#                 f"{os.environ['POSTGRES_PORT']}/"
#                 f"{os.environ['POSTGRES_DATABASE']}")
#             self.engine = create_engine(DATABASE_URI)
#         else:
#             logger.info(f"{str(MoexAuthenticationError)}")
#             raise MoexAuthenticationError()
#
#     def finish_get_data(self, df: pd.DataFrame, method: Request.__annotations__):
#         if df.empty:
#             raise AirflowSkipException("Skipping this task as DataFrame is empty")
#         df.columns = df.columns.str.lstrip('@')
#         df.columns = df.columns.str.lower()
#         self.engine.execute(
#             sa_text(f'''TRUNCATE TABLE {method.table_name}''').execution_options(autocommit=True))
#         df.to_sql(name=method.table_name,
#                   con=self.engine,
#                   if_exists='append',
#                   index=False)
#         logger.info(f"{method.table_name} downloaded successfully")
#
#     def get_stock_shares_boards(self):
#
#         iss = MicexISSClientBoards(self.my_config, self.my_auth)
#         url = StockSharesBoards().url()
#         response = self.opener.open(url)
#         boards = response.read().decode('utf-8')
#         data = xmltodict.parse(boards)
#         df = pd.DataFrame(data['document']['data']['rows']['row'])
#         self.finish_get_data(df, Boards)
#
#     def get_prices(self):
#         boardids = (
#             pd.read_sql(
#                 """
#                 SELECT DISTINCT boardid
#                 FROM public_marts.dim_moex_boards
#                 WHERE is_traded = true
#                 """
#                 , self.engine
#             )['boardid']
#             .to_list()
#         )
#         iss = MicexISSClientPrices(self.my_config, self.my_auth)
#         moex_prices_df: pd.DataFrame = pd.DataFrame()
#         for boardid in boardids:
#             df: pd.DataFrame = iss.get_data(boardid)
#             moex_prices_df = pd.concat([moex_prices_df, df], axis=0)
#         self.finish_get_data(moex_prices_df, Prices)
#
#     def get_securities_info(self):
#         # shares_list = (
#         #     pd.read_sql(
#         #         """
#         #         SELECT DISTINCT UPPER(secid) AS secid
#         #         FROM public_marts.dim_moex_shares
#         #         WHERE boardid in (
#         #             'EQBR', 'EQBS', 'EQDE', 'EQDP', 'EQLI', 'EQLV', 'EQNE', 'EQNL', 'EQTD', 'MPBB', 'MTQR', 'SMAL',
#         #             'SPEQ', 'TQBR', 'TQBS', 'TQDE', 'TQDP', 'TQFD', 'TQFE', 'TQIF', 'TQLV', 'TQNE', 'TQNL', 'TQPI',
#         #             'TQTD', 'TQTE', 'TQTF', 'TQTY') AND
#         #             secid != ''
#         #         ORDER BY secid
#         #         """
#         #         , self.engine
#         #     )['secid']
#         #     .to_list()
#         # )
#         iss = MicexISSClientShares(self.my_config, self.my_auth)
#         shares: pd.DataFrame = iss.get_data()
#         return shares
#         # if shares.empty:
#         #     logger.exception(f"Empty DataFrame for securities {securities}")
#         # for security_chunk in divide_chunks(securities, 10):
#         #     df: pd.DataFrame = iss.get_data(security_chunk)
#         #     if df.empty:
#         #         logger.exception(f"Empty DataFrame for securities {security_chunk}")
#         #     else:
#         #         shares = pd.concat([shares, df], axis=0)
#         #     sleep(1)
#         # self.finish_get_data(shares, Shares)
