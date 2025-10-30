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

    _instance = None

    def __init__(self, config):
        if self._instance.__initialized:
            return
        self._instance.__initialized = True

        self.config = config
        self.cookie_jar = http.cookiejar.CookieJar()
        self.auth()

    @staticmethod
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.__initialized = False
        return cls._instance

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
