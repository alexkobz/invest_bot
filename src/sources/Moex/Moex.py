from __future__ import annotations

import os
from datetime import date, timedelta

import pandas as pd
import xmltodict
from airflow.exceptions import AirflowSkipException
from dotenv import load_dotenv

from src.logger.Logger import Logger
from src.sources.Moex.iss_client import Config, MicexAuth
from src.sources.Moex.MoexAuthenticationError import MoexAuthenticationError
from src.utils.path import Path, get_dotenv_path
from src.utils.retries import retry

from ...postgres.StageSaver import StageSaver
from .MoexDocs import *

SCHEMA: str = 'moex'

logger = Logger()

dotenv_path: Path = get_dotenv_path()
load_dotenv(dotenv_path=dotenv_path)


class MoexStageSaver(StageSaver):
    def __init__(self, **kwargs):
        super().__init__(schema=SCHEMA, **kwargs)

    def before_save(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            raise AirflowSkipException("Skipping this task as DataFrame is empty")
        df.columns = df.columns.str.lstrip('@')
        df.columns = df.columns.str.lower()
        return df


class Moex:

    _instance = None

    def __init__(self):
        if self._instance.__initialized:
            return
        self._instance.__initialized = True
        self.config = None
        self.auth = None
        self._init_iss()

    @staticmethod
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.__initialized = False
        return cls._instance

    def _init_iss(self):
        self.config: Config = Config(
            user=os.environ["EMAIL_LOGIN"],
            password=os.environ["EMAIL_PASSWORD"],
            proxy_url='')
        self.auth: MicexAuth = MicexAuth(self.config)
        if self.auth.is_real_time():
            logger.info(f"Moex auth is successfully initialized")
        else:
            logger.exception(f"{str(MoexAuthenticationError)}")
            raise MoexAuthenticationError()
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
        response = self.opener.open(url, timeout=60)
        if response.code == 200:
            return response.read().decode('utf-8')
        else:
            logger.exception(response.status_code)
            raise AirflowSkipException(response.status_code)

    def _getStockSharesSecurities(self) -> list[dict]:
        url = StockSharesSecurities().url()
        res = self._send_request(url)
        res = xmltodict.parse(res)
        data: list[dict] = res['document']['data']
        return data

    @MoexStageSaver(table_name='StockSharesSecurities')
    def getStockSharesSecurities(self) -> pd.DataFrame:
        data = self._getStockSharesSecurities()
        securities = data[0]
        securities_df = pd.DataFrame(securities['rows']['row'])
        return securities_df

    @MoexStageSaver(table_name='StockSharesSecuritiesMarketData')
    def getStockSharesSecuritiesMarketData(self) -> pd.DataFrame:
        data = self._getStockSharesSecurities()
        marketdata = data[1]
        marketdata_df = pd.DataFrame(marketdata['rows']['row'])
        return marketdata_df

    @MoexStageSaver(table_name='StockSharesBoards')
    def getStockSharesBoards(self) -> pd.DataFrame:
        url = StockSharesBoards().url()
        res = self._send_request(url)
        res = xmltodict.parse(res)
        data = res['document']['data']
        df = pd.DataFrame(data['rows']['row'])
        return df

    @MoexStageSaver(table_name='StockSharesTQBRTrades')
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
        return trades_df

    @MoexStageSaver(table_name='Companies')
    def getCompanies(self) -> pd.DataFrame:
        """Companies Russia"""
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

    @MoexStageSaver(table_name='HistoryStockSharesSecurities')
    def getHistoryStockSharesSecurities(self, param_date: date = None) -> pd.DataFrame:
        securities_df = self._getHistoryStockSharesSecurities(param_date)
        return securities_df

    @MoexStageSaver(table_name='HistoryStockSharesSecurities')
    def getHistoryStockSharesSecuritiesCurrentMonth(self) -> pd.DataFrame:
        """Prices for current month"""
        res = pd.DataFrame()
        for param_date in pd.date_range(date.today().replace(day=1), date.today()):
            securities_df = self._getHistoryStockSharesSecurities(param_date)
            res = pd.concat([res, securities_df])
        return res

    @MoexStageSaver(table_name='HistoryStockSharesSecurities')
    def getHistoryStockSharesSecuritiesLastMonth(self) -> pd.DataFrame:
        """Prices for last month"""
        res = pd.DataFrame()
        for param_date in pd.date_range(date.today() - timedelta(days=30), date.today()):
            securities_df = self._getHistoryStockSharesSecurities(param_date)
            res = pd.concat([res, securities_df])
        return res

    @MoexStageSaver(table_name='HistoryStockSharesSecurities')
    def getHistoryStockSharesSecuritiesCurrentYear(self) -> pd.DataFrame:
        """Prices for current year"""
        res = pd.DataFrame()
        for param_date in pd.date_range(date(date.today().year, 1, 1), date.today()):
            securities_df = self._getHistoryStockSharesSecurities(param_date)
            res = pd.concat([res, securities_df])
        return res

    @MoexStageSaver(table_name='HistoryStockSharesSecurities')
    def getHistoryStockSharesSecuritiesLastYear(self) -> pd.DataFrame:
        """Prices for last year"""
        res = pd.DataFrame()
        for param_date in pd.date_range(date.today() - timedelta(days=365), date.today()):
            securities_df = self._getHistoryStockSharesSecurities(param_date)
            res = pd.concat([res, securities_df])
        return res

    @MoexStageSaver(table_name='HistoryStockSharesSecurities')
    def getHistoryStockSharesSecuritiesYear(self, year: int) -> pd.DataFrame:
        """Prices for distinct year"""
        res = pd.DataFrame()
        for param_date in pd.date_range(date(year, 1, 1), date(year, 12, 31)):
            securities_df = self._getHistoryStockSharesSecurities(param_date)
            res = pd.concat([res, securities_df])
        return res

    @MoexStageSaver(table_name='HistoryStockSharesSecurities')
    def getHistoryStockSharesSecuritiesRange(self, start: date, end: date) -> pd.DataFrame:
        """Prices for range of dates"""
        res = pd.DataFrame()
        for param_date in pd.date_range(start, end):
            securities_df = self._getHistoryStockSharesSecurities(param_date)
            res = pd.concat([res, securities_df])
        return res
