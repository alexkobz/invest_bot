"""
https://www.moex.com/a2920
"""

from typing import Type, List
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from datetime import datetime as dt, timedelta

# from functions.get_date import last_day_month_str, last_work_date_month, last_work_date_month_str


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
    Shares
    """
    url: str = 'https://iss.moex.com/iss/engines/stock/markets/shares/boards.xml'
    table_name: str = "api_moex_boards"
    


        
    # """
    # Shares
    # """
    # url: str = 'http://iss.moex.com/iss/history/engines/%(engine)s/markets/%(market)s/boards/%(board)s/securities.json?date=%(date)s'
    # engine: str = "stock"
    # market: str = "shares"
    # board: str = "eqne"
    # date: str = "2010-04-29"
    # table_name: str = "stg_moex_shares_prices"
