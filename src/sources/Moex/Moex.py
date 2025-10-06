from __future__ import annotations

import os
from airflow.exceptions import AirflowSkipException
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import date
import pandas as pd
import xmltodict
from .MoexDocs import *
from .iss_client import MicexAuth, Config
from src.utils.path import Path, get_project_root, get_dotenv_path
from src.sources.Moex.MoexAuthenticationError import MoexAuthenticationError
from logs.Logger import Logger
from src.utils.retries import retry

SCHEMA = 'moex'

logger = Logger()

class Moex:

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
        dotenv_path: Path = get_dotenv_path()
        load_dotenv(dotenv_path=dotenv_path)
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
            self.engine = create_engine(
                DATABASE_URI,
                connect_args={"options": f"-csearch_path={SCHEMA}"}
            )
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

    @retry(tries=5, delay=60)
    def _send_request(self, url: str) -> str:
        response = self.opener.open(url)
        if response.code == 200:
            return response.read().decode('utf-8')
        else:
            logger.exception(response.status_code)
            raise AirflowSkipException(response.status_code)

    def _finish_get_data(self, df: pd.DataFrame, table_name: str) -> bool:
        if df.empty:
            raise AirflowSkipException("Skipping this task as DataFrame is empty")
        try:
            df.columns = df.columns.str.lstrip('@')
            df.columns = df.columns.str.lower()
            self.engine.execute(f'''TRUNCATE TABLE "{table_name}"''' , autocommit=True)
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists='append',
                index=False)
            logger.info(f"{table_name} downloaded successfully")
            return True
        except Exception as e:
            logger.exception(f"{table_name} failed to download due to\n{e}")
            return False

    def getStockSharesSecurities(self) -> pd.DataFrame:
        url = StockSharesSecurities().url()
        res = self._send_request(url)
        res = xmltodict.parse(res)
        data = res['document']['data']
        try:
            securities = data[0]
        except IndexError:
            logger.exception('No securities found')
            return pd.DataFrame()
        securities_df = pd.DataFrame(securities['rows']['row'])
        self._finish_get_data(securities_df, 'StockSharesSecurities')
        try:
            marketdata = data[1]
            marketdata_df = pd.DataFrame(marketdata['rows']['row'])
            self._finish_get_data(marketdata_df, 'StockSharesSecuritiesMarketData')
        except IndexError:
            logger.exception('No marketdata found')
        return securities_df

    def getStockSharesBoards(self) -> pd.DataFrame:
        url = StockSharesBoards().url()
        res = self._send_request(url)
        res = xmltodict.parse(res)
        data = res['document']['data']
        df = pd.DataFrame(data['rows']['row'])
        self._finish_get_data(df, 'StockSharesBoards')
        return df

    def getStockSharesTQBRTrades(self) -> pd.DataFrame:
        limit = 5000
        start = 0
        trades_df = pd.DataFrame()
        while True:
            url = StockSharesTQBRTrades().url(params={'limit': limit, 'start': start})
            res = self._send_request(url)
            start += limit
            res = xmltodict.parse(res)
            data = res['document']['data']
            trades = data[0]
            try:
                df = pd.DataFrame(trades['rows']['row'])
            except:
                break
            trades_df = pd.concat([trades_df, df])
        self._finish_get_data(trades_df, 'StockSharesTQBRTrades')
        return trades_df

    def getCompanies(self) -> pd.DataFrame:
        limit = 1000
        start = 0
        companies_df = pd.DataFrame()
        while True:
            url = Companies().url(params={'limit': limit, 'start': start})
            res = self._send_request(url)
            start += limit
            res = xmltodict.parse(res)
            data = res['document']['data']
            companies = data[0]
            try:
                df = pd.DataFrame(companies['rows']['row'])
            except:
                break
            companies_df = pd.concat([companies_df, df])
        self._finish_get_data(companies_df, 'Companies')
        return companies_df

    def _getHistoryStockSharesSecurities(self, param_date: date = None) -> pd.DataFrame:
        limit = 100
        start = 0
        securities_df = pd.DataFrame()
        while True:
            if param_date is not None:
                url = HistoryStockSharesSecurities().url(
                    params={'limit': limit, 'start': start, 'date': param_date.strftime('%Y-%m-%d')})
            else:
                url = HistoryStockSharesSecurities().url(
                    params={'limit': limit, 'start': start})
            res = self._send_request(url)
            start += limit
            res = xmltodict.parse(res)
            data = res['document']['data']
            securities = data[0]
            try:
                df = pd.DataFrame(securities['rows']['row'])
            except:
                break
            securities_df = pd.concat([securities_df, df])
        return securities_df

    def getHistoryStockSharesSecurities(self, param_date: date = None) -> pd.DataFrame:
        securities_df = self._getHistoryStockSharesSecurities(param_date)
        self._finish_get_data(securities_df, 'HistoryStockSharesSecurities')
        return securities_df


    def getHistoryStockSharesSecuritiesLastMonth(self) -> pd.DataFrame:
        res = pd.DataFrame()
        for param_date in pd.date_range(date.today().replace(day=1), date.today()):
            securities_df = self._getHistoryStockSharesSecurities(param_date)
            res = pd.concat([res, securities_df])
        self._finish_get_data(res, 'HistoryStockSharesSecurities')
        return res

    def getHistoryStockSharesSecuritiesLastYear(self) -> pd.DataFrame:
        res = pd.DataFrame()
        for param_date in pd.date_range(date(date.today().year, 1, 1), date.today()):
            securities_df = self._getHistoryStockSharesSecurities(param_date)
            res = pd.concat([res, securities_df])
        self._finish_get_data(res, 'HistoryStockSharesSecurities')
        return res

    def getHistoryStockSharesSecuritiesYear(self, year: int) -> pd.DataFrame:
        res = pd.DataFrame()
        for param_date in pd.date_range(date(year, 1, 1), date(year, 12, 31)):
            securities_df = self._getHistoryStockSharesSecurities(param_date)
            res = pd.concat([res, securities_df])
        self._finish_get_data(res, 'HistoryStockSharesSecurities')
        return res

    def getHistoryStockSharesSecuritiesRange(self, start: date, end: date) -> pd.DataFrame:
        res = pd.DataFrame()
        for param_date in pd.date_range(start, end):
            securities_df = self._getHistoryStockSharesSecurities(param_date)
            res = pd.concat([res, securities_df])
        self._finish_get_data(res, 'HistoryStockSharesSecurities')
        return res
