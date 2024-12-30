"""
https://www.moex.com/a2920
"""

from typing import Type, List
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from datetime import datetime as dt, timedelta

from src.get_date import yesterday_str


class Request:

    @property
    def url(self) -> str:
        return self.__dict__["url"]

    @property
    def table_name(self) -> str:
        return self.__dict__["table_name"]


class Shares(Request):
    
    @property
    def engine(self) -> str:
        return self.__dict__["engine"]
        
    @property
    def market(self) -> str:
        return self.__dict__["market"]
        
    @property
    def board(self) -> str:
        return self.__dict__["board"]
        
    @property
    def date(self) -> str:
        return self.__dict__["date"]


@dataclass
class Boards(Request):
    """
    Boards
    """
    url: str = 'https://iss.moex.com/iss/engines/stock/markets/shares/boards.xml'
    table_name: str = "api_moex_boards"


@dataclass
class SecuritiesStock(Request):
    """
    Securities with engine=stock
    """
    url: str = 'https://iss.moex.com/iss/securities.xml?engine=stock'
    table_name: str = "api_moex_securities_stock"


@dataclass
class SecuritiesTrading(Request):
    """
    Shares trading
    """
    url: str = 'https://iss.moex.com/iss/securities.xml?is_trading=1'
    table_name: str = "api_moex_securities_trading"


@dataclass
class Securities(Request):
    """
    Securities
    """
    url: str = 'https://iss.moex.com/iss/securities.xml'
    table_name: str = "api_moex_securities"


@dataclass
class Prices(Shares):
    """
    Shares
    """
    url: str = 'http://iss.moex.com/iss/history/engines/%(engine)s/markets/%(market)s/boards/%(board)s/securities.json?date=%(date)s'
    engine: str = "stock"
    market: str = "shares"
    board: str = ""
    # date: str = '2024-12-27'
    date: str = yesterday_str
    table_name: str = "api_moex_prices"
