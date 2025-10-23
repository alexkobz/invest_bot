from abc import ABC, abstractmethod, abstractproperty

import overrides
import requests
import pandas as pd
from pysimplesoap.client import SoapClient
from collections import OrderedDict
import xmltodict
import xml.etree.ElementTree as ET
from contextlib import suppress
from datetime import datetime as dt, date, timedelta

from requests import Response


# from src.utils.postgres_engine import postgres_engine


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
        # self.engine = postgres_engine()

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
        return response

    @abstractmethod
    def _parse_response(self, response):
        raise NotImplementedError()

    def save_df(self, df: pd.DataFrame) -> None:
        df.to_sql(
            name=self.method,
            con=self.engine,
            if_exists='replace',
            index=False
        )

    def run(self):
        root = self.send_request()
        df: pd.DataFrame = self._parse_response(root)
        # self.save_df(df)
        return df


class KeyRate(CBR):
    """
    https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?op=KeyRate
    """

    def __init__(
        self,
        from_date: str = '2013-09-16T00:00:00',
        to_date: str = date.today().strftime('%Y-%m-%dT23:59:59')
    ):
        super().__init__()
        self.params: dict[str, str] = {
            'fromDate': from_date,
            'ToDate': to_date
        }

    def _parse_response(self, response) -> pd.DataFrame:
        # Parse XML
        root = ET.fromstring(response.text)
        entries = root.findall(".//KR", CBR.namespaces)

        # Extract values
        result = []
        for kr in entries:
            DT = kr.find("DT").text
            rate = float(kr.find("Rate").text)
            result.append({"date": DT, "rate": rate})

        df = pd.DataFrame(result)
        # Data cleaning and type conversion
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'], utc=True).dt.date
            df['rate'] = pd.to_numeric(df['rate'], errors='coerce')

        return df


class GetCursOnDate(CBR):
    """
    https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?op=GetCursOnDate
    """

    def __init__(
        self,
        on_date: str = date.today().strftime('%Y-%m-%d')
    ):
        super().__init__()
        self.params: dict[str, str] = {
            'On_date': on_date
        }

    def _parse_response(self, response) -> pd.DataFrame:
        # Parse XML
        root = ET.fromstring(response.text)
        entries = root.findall('.//diffgr:diffgram//ValuteCursOnDate', CBR.namespaces)

        # Extract values
        currencies = []
        for val in entries:
            currency = {
                'name': val.find('Vname').text.strip() if val.find('Vname') is not None else None,
                'nom': float(val.find('Vnom').text) if val.find('Vnom') is not None else None,
                'curs': float(val.find('Vcurs').text) if val.find('Vcurs') is not None else None,
                'code': val.find('Vcode').text if val.find('Vcode') is not None else None,
                'chCode': val.find('VchCode').text if val.find('VchCode') is not None else None,
                'unitRate': float(val.find('VunitRate').text) if val.find('VunitRate') is not None else None,
                'date': self.params['On_date']
            }
            currencies.append(currency)

        # В виде pandas DataFrame
        df = pd.DataFrame(currencies)
        return df


class MainInfoXML(CBR):

    def _parse_response(self, response):
        return response


class AllDataInfoXML(CBR):

    def _parse_response(self, response):
        return response
