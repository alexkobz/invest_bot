"""
https://tinkoff.github.io/investAPI/swagger-ui/#/
"""

from __future__ import annotations

import urllib.parse
import urllib.request
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

BASE = "https://invest-public-api.tbank.ru/rest"


@dataclass
class Endpoint(ABC):
    """Абстрактный dataclass для MOEX ISS endpoints."""
    path: str

    @abstractmethod
    def payload(self):
        raise NotImplementedError()

    def url(self, base: str = BASE, params: Optional[Dict[str, Any]] = None) -> str:
        """Построить полный URL. Подставляет поля dataclass в path."""
        full = base.rstrip("/") + "/" + self.path.lstrip("/")
        if params:
            full = full.format(**params)
            query = urllib.parse.urlencode(params, doseq=True)
            full = f"{full}?{query}"
        return full

"""InstrumentsService"""
@dataclass
class Bonds(Endpoint):
    path = "/tinkoff.public.invest.api.contract.v1.InstrumentsService/Bonds"

@dataclass
class Shares(Endpoint):
    path = "/tinkoff.public.invest.api.contract.v1.InstrumentsService/Shares"

@dataclass
class Etfs(Endpoint):
    path = "/tinkoff.public.invest.api.contract.v1.InstrumentsService/Etfs"

@dataclass
class Futures(Endpoint):
    path = "/tinkoff.public.invest.api.contract.v1.InstrumentsService/Futures"

@dataclass
class Options(Endpoint):
    path = "/tinkoff.public.invest.api.contract.v1.InstrumentsService/OptionsBy"


"""MarketDataService"""
@dataclass
class getClosePrices(Endpoint):
    path = "/tinkoff.public.invest.api.contract.v1.MarketDataService/GetClosePrices"

    def payload(self):
        return {
          "instruments": [
            {
              "instrumentId": "string"
            }
          ]
        }


"""OperationsService"""

"""OrdersService"""
@dataclass
class PostOrder(Endpoint):
    path = "/tinkoff.public.invest.api.contract.v1.OrdersService/PostOrder"

@dataclass
class CancelOrder(Endpoint):
    path = "/tinkoff.public.invest.api.contract.v1.OrdersService/CancelOrder"

@dataclass
class GetOrder(Endpoint):
    path = "/tinkoff.public.invest.api.contract.v1.OrdersService/GetOrder"

@dataclass
class ReplaceOrder(Endpoint):
    path = "/tinkoff.public.invest.api.contract.v1.OrdersService/ReplaceOrder"

"""StopOrdersService"""
"""UsersService"""
"""MarketDataStreamService"""
"""OrdersStreamService"""
