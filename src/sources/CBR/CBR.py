import os

import certifi
import requests
import pandas as pd
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from sqlalchemy import create_engine
from zeep import Client
from zeep.proxy import ServiceProxy
from zeep.transports import Transport

from src.logger.Logger import Logger
from src.utils.path import get_dotenv_path, Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from zeep import Client, Transport
import urllib3


SCHEMA = 'cbr'

logger = Logger()
dotenv_path: Path = get_dotenv_path()
load_dotenv(dotenv_path=dotenv_path)


class CBR(ABC):

    url = 'https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?WSDL'

    def __init__(self):
        DATABASE_URI: str = (
            f"postgresql://"
            f"{os.environ['POSTGRES_USER']}:"
            f"{os.environ['POSTGRES_PASSWORD']}@"
            # f"{os.environ['POSTGRES_HOST']}:"
            f"host.docker.internal:"
            # f"172.18.0.1:"
            f"{os.environ['POSTGRES_PORT']}/"
            f"{os.environ['POSTGRES_DATABASE']}")
        self.engine = create_engine(
            DATABASE_URI,
            connect_args={"options": f"-csearch_path={SCHEMA}"}
        )
        self.service = None
        self.df: pd.DataFrame = pd.DataFrame()

    def get_service(self) -> ServiceProxy:
        session = requests.session()
        session.verify = certifi.where()

        # retry_strategy = Retry(
        #     total=3,
        #     backoff_factor=1,
        #     status_forcelist=[429, 500, 502, 503, 504],
        # )
        # adapter = HTTPAdapter(max_retries=retry_strategy)
        # session.mount("http://", adapter)
        # session.mount("https://", adapter)
        # session.verify = False
        # session.timeout = 10
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

    def save_df(self) -> bool:
        if self.df is not None or not self.df.empty:
            self.engine.execute(f'''TRUNCATE TABLE "{self.__class__.__name__}"''' , autocommit=True)
            self.df.to_sql(
                name=self.__class__.__name__,
                con=self.engine,
                if_exists='append',
                index=False
            )
            return True
        return False

    def run(self):
        self.get_service()
        self.parse_response()
        self.save_df()
        return self.df
