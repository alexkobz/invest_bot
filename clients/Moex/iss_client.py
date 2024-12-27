#!/usr/bin/env python3
"""
Library implementing interaction with Moscow Exchange ISS server.

Version: 1.2
Refactored for Python 3.

@copyright: 2016 by MOEX
"""

import urllib.request
import urllib.parse
import base64
import http.cookiejar
import json
from typing import override

import pandas as pd
import xmltodict
from sqlalchemy import create_engine
from clients.Moex.MoexAPI import Request


class Config:
    def __init__(self, user='', password='', proxy_url='', debug_level=0):
        """Container for all the configuration options."""
        self.debug_level = debug_level
        self.proxy_url = proxy_url
        self.user = user
        self.password = password
        self.auth_url = "https://passport.moex.com/authenticate"


class MicexAuth:
    """User authentication data and functions."""

    def __init__(self, config):
        self.config = config
        self.cookie_jar = http.cookiejar.CookieJar()
        self.auth()

    def auth(self):
        """Authenticate the user."""
        if self.config.proxy_url:
            opener = urllib.request.build_opener(
                urllib.request.ProxyHandler({"http": self.config.proxy_url}),
                urllib.request.HTTPCookieProcessor(self.cookie_jar),
                urllib.request.HTTPHandler(debuglevel=self.config.debug_level),
            )
        else:
            opener = urllib.request.build_opener(
                urllib.request.HTTPCookieProcessor(self.cookie_jar),
                urllib.request.HTTPHandler(debuglevel=self.config.debug_level),
            )
        auth_string = f"{self.config.user}:{self.config.password}"
        encoded_auth = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')
        opener.addheaders = [('Authorization', f'Basic {encoded_auth}')]

        try:
            opener.open(self.config.auth_url)
        except Exception as e:
            print(f"Authentication failed: {e}")

        # Check for the 'MicexPassportCert' cookie
        self.passport = None
        for cookie in self.cookie_jar:
            if cookie.name == 'MicexPassportCert':
                self.passport = cookie
                break
        if self.passport is None:
            print("Cookie not found!")

    def is_real_time(self):
        """Check if the cookie is valid."""
        if not self.passport or self.passport.is_expired():
            self.auth()
        return bool(self.passport and not self.passport.is_expired())


class MicexISSDataHandler:
    """Data handler which will be called by the ISS client to handle downloaded data."""

    def __init__(self, DATABASE_URI: str):
        self.engine = create_engine(DATABASE_URI)
        self.data = []

    def insert(self, result, table_name):
        """Process the data."""
        result.to_sql(table_name, con=self.engine, if_exists='append', index=False)


class MicexISSClient:
    """Methods for interacting with the MICEX ISS server."""

    def __init__(self, config, auth):
        if config.proxy_url:
            self.opener = urllib.request.build_opener(
                urllib.request.ProxyHandler({"http": config.proxy_url}),
                urllib.request.HTTPCookieProcessor(auth.cookie_jar),
                urllib.request.HTTPHandler(debuglevel=config.debug_level),
            )
        else:
            self.opener = urllib.request.build_opener(
                urllib.request.HTTPCookieProcessor(auth.cookie_jar),
                urllib.request.HTTPHandler(debuglevel=config.debug_level),
            )
        urllib.request.install_opener(self.opener)

    def data(self, method: Request) -> pd.DataFrame:
        raise NotImplementedError("Subclasses must implement this method.")


class MicexISSClientHistorySecurities(MicexISSClient):

    @override
    def get_data(self, method: Request) -> pd.DataFrame:
        """Get and parse historical data."""
        url = method.url % {
            'engine': method.engine,
            'market': method.market,
            'board': method.board,
            'date': method.date,
        }

        start = 0
        result = pd.DataFrame()
        while True:
            res = self.opener.open(f"{url}&start={start}")
            jres = json.load(res)

            jhist = jres['history']
            jdata = jhist['data']
            jcols = jhist['columns']
            print(len(jdata))
            if not jdata:
                break

            chunk = pd.DataFrame(data=jdata, columns=jcols)
            result = pd.concat([result, chunk], axis=0)
            start += len(jdata)
        return result


class MicexISSClientBoards(MicexISSClient):

    @override
    def get_data(self, method: Request) -> pd.DataFrame:
        """Get and parse historical data."""
        url = method.url
        response = self.opener.open(url)
        boards = response.read().decode('utf-8')
        data = xmltodict.parse(boards)
        result = pd.DataFrame(data['document']['data']['rows']['row'])
        return result


def del_null(num):
    """Replace null with zero."""
    return 0 if num is None else num
