from enum import IntEnum
from typing import List
from dataclasses import dataclass, field


class Request:

    @property
    def url(self) -> dict:
        return self.__dict__["url"]

    @property
    def requestType(self) -> dict:
        return self.__dict__["requestType"]

    def payload(self) -> dict:
        return {
            key.replace("_", ""): value
            for key, value in self.__dict__.items()
            if key not in ("url", "requestType")
        }


class RequestType(IntEnum):
    INSTRUMENT = 100


@dataclass
class GetAssetFundamentals(Request):
    url: str = 'https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService/GetAssetFundamentals'
    requestType: RequestType = RequestType.INSTRUMENT
    assets: List[str] = field(default_factory=lambda: [])
