import os
from abc import ABC, abstractmethod
from xml.etree.ElementTree import Element

import requests
import pandas as pd
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from requests import Response
from sqlalchemy import create_engine

from src.logger.Logger import Logger
from src.utils.path import get_dotenv_path, Path


SCHEMA = 'cbr'

logger = Logger()
dotenv_path: Path = get_dotenv_path()
load_dotenv(dotenv_path=dotenv_path)


class CBR(ABC):

    url = 'https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx'
    # Define namespaces
    namespaces = {
        "soap": "http://schemas.xmlsoap.org/soap/envelope/",
        "cbr": "http://web.cbr.ru/",
        "diffgr": "urn:schemas-microsoft-com:xml-diffgram-v1",
        "msdata": "urn:schemas-microsoft-com:xml-msdata"
    }

    def __init__(self):
        self.params: dict[str, str] = {}
        self.method = self.__class__.__name__
        self.headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": f"http://web.cbr.ru/{self.method}"
        }
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
        self.response = None

    @property
    def body(self):
        params_xml = "".join(f"<{k}>{v}</{k}>" for k, v in self.params.items())

        body = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                       xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                       xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
          <soap:Body>
            <{self.method} xmlns="http://web.cbr.ru/">
              {params_xml}
            </{self.method}>
          </soap:Body>
        </soap:Envelope>""".encode('utf-8')
        return body

    def send_request(self) -> Response:
        response = requests.post(self.url, data=self.body, headers=self.headers)
        response.raise_for_status()
        self.response: Response = response
        return response

    def get_element(self) -> Element:
        if self.response is None:
            self.response = self.send_request()
        return ET.fromstring(self.response.text)

    @abstractmethod
    def _parse_response(self, root: Element) -> pd.DataFrame:
        raise NotImplementedError()

    def save_df(self, df: pd.DataFrame) -> None:
        df.to_sql(
            name=self.method,
            con=self.engine,
            if_exists='replace',
            index=False
        )

    def run(self):
        self.send_request()
        root = self.get_element()
        df: pd.DataFrame = self._parse_response(root)
        self.save_df(df)
        return df
