import asyncio
import os

import aiohttp
from time import sleep
from typing import Dict

from logs.Logger import Logger
from clients.RuData.DocsAPI import LIMIT
from clients.RuData.Token import Token


logger = Logger()

class RuDataRequest:
    """
    Class for sending requests to RU DATA and getting responses
    """

    headers: Dict[str, str] = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/39.0.2171.95 Safari/537.36',
        'content-type': 'application/json',
        'Accept': 'application/json',
    }
    semaphore: asyncio.Semaphore = asyncio.Semaphore(LIMIT)

    def __init__(self, url: str, session: aiohttp.ClientSession):
        self.url: str = url
        self.session: aiohttp.ClientSession = session

    @staticmethod
    def set_headers() -> None:
        token: str = str(Token())
        RuDataRequest.headers["Authorization"] = 'Bearer ' + token

    async def post(self, payload):
        async with RuDataRequest.semaphore, self.session.post(
                self.url,
                json=payload,
                headers=RuDataRequest.headers,
                timeout=60
        ) as response:
            if response.ok:
                try:
                    response_body = await response.json()
                    await asyncio.sleep(1.1)
                except aiohttp.client_exceptions.ClientConnectorError as e:
                    logger.exception(str(e))
                    sleep(10)
                    raise ConnectionError("Restart")
                except asyncio.exceptions.TimeoutError as e:
                    logger.exception(str(e))
                    sleep(10)
                    await self.post(payload)
                except Exception as e:
                    logger.exception(str(e))
                    sleep(60)
                    raise Exception("Restart")
                finally:
                    return response_body
            else:
                return []
