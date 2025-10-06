"""
https://iss.moex.com/iss/reference/
"""

from __future__ import annotations
import urllib.request
import urllib.parse
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any

BASE = "https://iss.moex.com"


@dataclass
class Endpoint(ABC):
    """Абстрактный dataclass для MOEX ISS endpoints."""
    path: str

    def url(self, base: str = BASE, params: Optional[Dict[str, Any]] = None) -> str:
        """Построить полный URL. Подставляет поля dataclass в path."""
        full = base.rstrip("/") + "/" + self.path.lstrip("/")
        if params:
            full = full.format(**params)
            query = urllib.parse.urlencode(params, doseq=True)
            full = f"{full}?{query}"
        return full


@dataclass
class Securities(Endpoint):
    """
    https://iss.moex.com/iss/reference/205
    Список бумаг, торгуемых на Московской бирже.
    """
    path = "/iss/securities"


@dataclass
class SecuritiesDetail(Securities):
    """
    https://iss.moex.com/iss/reference/193
    Детальная информация по конкретной бумаге.
    """
    path = "/iss/securities/{security}"
    security: str


@dataclass
class SecuritiesIndices(SecuritiesDetail):
    security: str
    @property
    def path(self) -> str:
        return "iss/securities/{security}/indices"

@dataclass
class SecuritiesAggregates(SecuritiesDetail):
    security: str
    @property
    def path(self) -> str:
        return "iss/securities/{security}/aggregates"

@dataclass
class Index(Endpoint):
    @property
    def path(self) -> str:
        return "iss/index"

@dataclass
class Turnovers(Endpoint):
    @property
    def path(self) -> str:
        return "iss/turnovers"

@dataclass
class TurnoversColumns(Turnovers):
    @property
    def path(self) -> str:
        return "iss/turnovers/columns"

# --- Engines / Markets group ---

@dataclass
class Engines(Endpoint):
    @property
    def path(self) -> str:
        return "iss/engines"

class Engine(Engines):
    engine: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}"

@dataclass
class Stock(Engine):
    engine: str = "stock"

class EngineMarkets(Endpoint):
    engine: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets"

@dataclass
class StockMarkets(EngineMarkets):
    engine: str = 'stock'

class Market(Endpoint):
    engine: str
    market: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}"

@dataclass
class StockShares(Market):
    engine: str = 'stock'
    market: str = 'shares'


class MarketSecstats(Endpoint):
    engine: str
    market: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/secstats"

@dataclass
class StockSharesSecstats(MarketSecstats):
    engine: str = 'stock'
    market: str = 'shares'


class EngineTurnovers(Endpoint):
    engine: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/turnovers"

@dataclass
class StockTurnovers(EngineTurnovers):
    engine: str = 'stock'


class MarketTurnovers(Endpoint):
    engine: str
    market: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/turnovers"

@dataclass
class StockSharesTurnovers(MarketTurnovers):
    engine: str = 'stock'
    market: str = 'shares'


class MarketZcyc(Endpoint):
    engine: str
    market: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/zcyc"


class EngineZcyc(Endpoint):
    engine: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/zcyc"

# --- Orders / Orderbook / Trades / Securities at market level ---

class MarketOrderbookColumns(Endpoint):
    engine: str
    market: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/orderbook/columns"


class MarketSecuritiesColumns(Endpoint):
    engine: str
    market: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/securities/columns"


class MarketTradesColumns(Endpoint):
    engine: str
    market: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/trades/columns"


class MarketSecurities(Endpoint):
    engine: str
    market: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/securities"

@dataclass
class StockSharesSecurities(MarketSecurities):
    """
    https://iss.moex.com/iss/reference/353
    Получить таблицу инструментов по режиму торгов.
    """
    path = "/iss/engines/stock/markets/shares/securities"


class MarketSecurity(Endpoint):
    engine: str
    market: str
    security: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/securities/{security}"


class SecurityTrades(Endpoint):
    engine: str
    market: str
    security: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/securities/{security}/trades"


class SecurityOrderbook(Endpoint):
    engine: str
    market: str
    security: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/securities/{security}/orderbook"


class MarketOrderbook(Endpoint):
    engine: str
    market: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/orderbook"


class MarketTrades(Endpoint):
    engine: str
    market: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/trades"


@dataclass
class StockSharesTrades(MarketTrades):
    engine: str = 'stock'
    market: str = 'shares'


class MarketBoards(Endpoint):
    engine: str
    market: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/boards"


class Board(Endpoint):
    engine: str
    market: str

    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/boards"


@dataclass
class StockSharesBoards(Board):
    """
    https://iss.moex.com/iss/reference/723
    Получить справочник режимов торгов рынка.
    """
    path = "/iss/engines/stock/markets/shares/boards"


class BoardSecurities(Endpoint):
    engine: str
    market: str
    board: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/boards/{board}/securities"


class BoardSecurity(Endpoint):
    engine: str
    market: str
    board: str
    security: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/boards/{board}/securities/{security}"


class BoardSecurityTrades(Endpoint):
    engine: str
    market: str
    board: str
    security: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/boards/{board}/securities/{security}/trades"


class BoardSecurityOrderbook(Endpoint):
    engine: str
    market: str
    board: str
    security: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/boards/{board}/securities/{security}/orderbook"


class SecurityCandles(Endpoint):
    engine: str
    market: str
    security: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/securities/{security}/candles"


class CandleBorders(Endpoint):
    engine: str
    market: str
    security: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/securities/{security}/candleborders"


class BoardGroupCandles(Endpoint):
    engine: str
    market: str
    boardgroup: str
    security: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/boardgroups/{boardgroup}/securities/{security}/candles"


class BoardSecurityCandles(Endpoint):
    engine: str
    market: str
    board: str
    security: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/boards/{board}/securities/{security}/candles"


class BoardSecurityCandleBorders(Endpoint):
    engine: str
    market: str
    board: str
    security: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/boards/{board}/securities/{security}/candleborders"


class BoardTrades(Endpoint):
    engine: str
    market: str
    board: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/boards/{board}/trades"


@dataclass
class StockSharesTQBRTrades(BoardTrades):
    """
    https://iss.moex.com/iss/reference/325
    Получить все сделки по выбранному режиму торгов.
    """
    path = "/iss/engines/stock/markets/shares/boards/tqbr/trades"


class BoardOrderbook(Endpoint):
    engine: str
    market: str
    board: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/boards/{board}/orderbook"

# --- Boardgroups ---

class BoardGroups(Endpoint):
    engine: str
    market: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/boardgroups"


class BoardGroup(Endpoint):
    engine: str
    market: str
    boardgroup: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/boardgroups/{boardgroup}"


class BoardGroupSecurities(Endpoint):
    engine: str
    market: str
    boardgroup: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/boardgroups/{boardgroup}/securities"


class BoardGroupSecurity(Endpoint):
    engine: str
    market: str
    boardgroup: str
    security: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/boardgroups/{boardgroup}/securities/{security}"


class BoardGroupSecurityTrades(Endpoint):
    engine: str
    market: str
    boardgroup: str
    security: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/boardgroups/{boardgroup}/securities/{security}/trades"


class BoardGroupSecurityOrderbook(Endpoint):
    engine: str
    market: str
    boardgroup: str
    security: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/boardgroups/{boardgroup}/securities/{security}/orderbook"


class BoardGroupTrades(Endpoint):
    engine: str
    market: str
    boardgroup: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/boardgroups/{boardgroup}/trades"


class BoardGroupOrderbook(Endpoint):
    engine: str
    market: str
    boardgroup: str
    @property
    def path(self) -> str:
        return "iss/engines/{engine}/markets/{market}/boardgroups/{boardgroup}/orderbook"

# --- History endpoints ---

class HistoryListingColumns(Endpoint):
    engine: str
    market: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/listing/columns"


class HistoryListing(Endpoint):
    engine: str
    market: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/listing"


class HistoryBoardListing(Endpoint):
    engine: str
    market: str
    board: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/boards/{board}/listing"


class HistoryBoardGroupListing(Endpoint):
    engine: str
    market: str
    boardgroup: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/boardgroups/{boardgroup}/listing"


class HistorySessions(Endpoint):
    engine: str
    market: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/sessions"


class HistorySessionSecurities(Endpoint):
    engine: str
    market: str
    session: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/sessions/{session}/securities"


class HistorySessionSecurity(Endpoint):
    engine: str
    market: str
    session: str
    security: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/sessions/{session}/securities/{security}"


class HistorySessionBoardGroupSecurities(Endpoint):
    engine: str
    market: str
    session: str
    boardgroup: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/session/{session}/boardgroups/{boardgroup}/securities"


class HistorySessionBoardGroupSecurity(Endpoint):
    engine: str
    market: str
    session: str
    boardgroup: str
    security: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/sessions/{session}/boardgroups/{boardgroup}/securities/{security}"


class HistorySessionBoardSecurities(Endpoint):
    engine: str
    market: str
    session: str
    board: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/sessions/{session}/boards/{board}/securities"


class HistorySessionBoardSecurity(Endpoint):
    engine: str
    market: str
    session: str
    board: str
    security: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/sessions/{session}/boards/{board}/securities/{security}"


class HistorySecuritiesColumns(Endpoint):
    engine: str
    market: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/securities/columns"

# ... продолжение: yields, dates, changeover, zcyc history и т.д. ---

class HistoryChangeover(Endpoint):
    @property
    def path(self) -> str:
        return "iss/history/engines/stock/securities/changeover"


class HistoryEngineZcyc(Endpoint):
    engine: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/zcyc"


class HistorySecurities(Endpoint):
    engine: str
    market: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/securities"


@dataclass
class HistoryStockSharesSecurities(HistorySecurities):
    """
    https://iss.moex.com/iss/reference/467
    Получить историю по всем бумагам на рынке за одну дату.
    """
    path = "/iss/history/engines/stock/markets/shares/securities"


class HistoryYields(Endpoint):
    engine: str
    market: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/yields"


class HistoryDates(Endpoint):
    engine: str
    market: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/dates"


class HistorySecurity(Endpoint):
    engine: str
    market: str
    security: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/securities/{security}"


class HistorySecurityYields(Endpoint):
    engine: str
    market: str
    security: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/yields/{security}"


class HistorySecurityDates(Endpoint):
    engine: str
    market: str
    security: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/securities/{security}/dates"


class HistoryBoardDates(Endpoint):
    engine: str
    market: str
    board: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/boards/{board}/dates"


class HistoryBoardYields(Endpoint):
    engine: str
    market: str
    board: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/boards/{board}/yields"


class HistoryBoardSecurity(Endpoint):
    engine: str
    market: str
    board: str
    security: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/boards/{board}/securities/{security}"


class HistoryBoardSecurityYields(Endpoint):
    engine: str
    market: str
    board: str
    security: str
    @property
    def path(self) -> str:
        return "iss/history/engines/{engine}/markets/{market}/boards/{board}/yields/{security}"

# --- Archives ---

class ArchivesYears(Endpoint):
    engine: str
    market: str
    datatype: str  # 'securities' or 'trades'
    @property
    def path(self) -> str:
        return "iss/archives/engines/{engine}/markets/{market}/{datatype}/years"


class ArchivesPeriod(Endpoint):
    engine: str
    market: str
    datatype: str
    period: str  # yearly|monthly|daily
    @property
    def path(self) -> str:
        return "iss/archives/engines/{engine}/markets/{market}/{datatype}/{period}"


class ArchivesMonths(Endpoint):
    engine: str
    market: str
    datatype: str
    year: int
    @property
    def path(self) -> str:
        return "iss/archives/engines/{engine}/markets/{market}/{datatype}/years/{year}/months"

# --- OTC history (examples from reference) ---

class HistoryOtcProvidersMarkets(Endpoint):
    @property
    def path(self) -> str:
        return "iss/history/otc/providers/nsd/markets"


class HistoryOtcMarketsDaily(Endpoint):
    market: str
    @property
    def path(self) -> str:
        return "iss/history/otc/providers/nsd/markets/{market}/daily"


class HistoryOtcMarketsMonthly(Endpoint):
    market: str
    @property
    def path(self) -> str:
        return "iss/history/otc/providers/nsd/markets/{market}/monthly"

@dataclass
class Companies(Endpoint):
    """
    https://iss.moex.com/iss/reference/881
    Сервисы корпоративной информации. Справочная информация по организациям.
    """
    path = "/iss/cci/info/companies"
