"""
https://www.moex.com/a2920

https://iss.moex.com/iss/index
Глобальные справочники
"""



from dataclasses import dataclass

from src.utils.get_date import yesterday_str


class Request:

    @property
    def url(self) -> str:
        return self.__dict__["url"]

    @property
    def table_name(self) -> str:
        return self.__dict__["table_name"]


class Securities(Request):
    
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
class Prices(Securities):
    """
    Shares
    """
    url: str = 'http://iss.moex.com/iss/history/engines/%(engine)s/markets/%(market)s/boards/%(board)s/securities.json?date=%(date)s'
    engine: str = "stock"
    market: str = "shares"
    board: str = ""
    date: str = yesterday_str
    table_name: str = "api_moex_prices"


@dataclass
class SettlementsCalendar(Request):
    """
    Settlements calendar
    """
    url: str = 'https://iss.moex.com/iss/rms/engines/stock/objects/settlementscalendar'
    table_name: str = "moex_settlements_calendar"


@dataclass
class Shares(Securities):
    """
    https://iss.moex.com/iss/reference/347
    """

    url: str = 'http://iss.moex.com/iss/engines/stock/markets/shares/securities'
    engine: str = "stock"
    market: str = "shares"
    table_name: str = "api_moex_shares"


"""https://iss.moex.com/iss/reference/449
Обобщенная информация по фондовому рынку
"""
'http://iss.moex.com/iss/history/engines/stock/totals/securities'

"""https://iss.moex.com/iss/reference/733
Сервисы корпоративной информации. Справочная информация по организациям.
"""
'http://iss.moex.com/iss/history/engines/stock/totals/securities'

"""https://iss.moex.com/iss/reference/805
Сервисы корпоративной информации. Базовая информация по всем выпускам.
"""
'https://iss.moex.com/iss/cci/info-nsd/securities'

"""
https://iss.moex.com/iss/reference/881
Сервисы корпоративной информации. Справочная информация по организациям.
"""
'https://iss.moex.com/iss/cci/info/companies'
