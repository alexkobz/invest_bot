import asyncio
from time import sleep
from typing import Dict, Any, List
import pandas as pd

from TinkoffClient import DocsAPI
from logger.Logger import Logger


class TinkoffDF:

    def __init__(self, key=None):
        if key:
            self.key: str = key
            self.url: str = getattr(DocsAPI, key).url
            self.payload: Dict[str, Any] = getattr(DocsAPI, key)().payload()
            self.requestType: DocsAPI.RequestType = getattr(DocsAPI, key).requestType
