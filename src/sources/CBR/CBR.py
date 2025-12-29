from abc import ABC, abstractmethod

import certifi
import pandas as pd
import requests
from zeep import Client, Transport
from zeep.proxy import ServiceProxy

from src.logger.Logger import Logger
from src.postgres.StageSaver import StageSaver

SCHEMA: str = 'cbr'

logger = Logger()


class CBRStageSaver(StageSaver):
    def __init__(self, **kwargs):
        super().__init__(schema=SCHEMA, **kwargs)


class CBR(ABC):

    url: str = 'https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?WSDL'

    def __init__(self):
        self.service = None
        self.df: pd.DataFrame = pd.DataFrame()

    def get_service(self) -> ServiceProxy:
        session = requests.session()
        session.verify = certifi.where()
        transport = Transport(session=session)
        client = Client(
            self.url,
            transport=transport
        )
        self.service = client.service
        return self.service

    @abstractmethod
    def parse_response(self) -> pd.DataFrame:
        raise NotImplementedError()

    def run(self) -> pd.DataFrame:
        self.get_service()
        self.parse_response()
        return self.df
