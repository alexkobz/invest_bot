import os
import pandas as pd
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from sqlalchemy import create_engine
from zeep import Client
from zeep.proxy import ServiceProxy

from src.logger.Logger import Logger
from src.utils.path import get_dotenv_path, Path


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
            f"{os.environ['POSTGRES_HOST']}:"
            f"{os.environ['POSTGRES_PORT']}/"
            f"{os.environ['POSTGRES_DATABASE']}")
        self.engine = create_engine(
            DATABASE_URI,
            connect_args={"options": f"-csearch_path={SCHEMA}"}
        )
        self.service = None
        self.df: pd.DataFrame = pd.DataFrame()


    def get_service(self) -> ServiceProxy:
        client = Client(self.url)
        self.service = client.service
        return self.service

    @abstractmethod
    def parse_response(self) -> pd.DataFrame:
        raise NotImplementedError()

    def save_df(self) -> bool:
        if self.df is not None or not self.df.empty:
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
