import aiohttp
import asyncio
import os
from time import sleep
from typing import Dict

from logger.Logger import Logger

logger = Logger()

class TinkoffRequest:
    """
    Class for sending requests to Tinkoff invest and getting responses
    """

    headers: Dict[str, str] = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/39.0.2171.95 Safari/537.36',
        'content-type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'Bearer ' + os.environ["SANDBOX_TOKEN"]
    }


    def __init__(self, url: str, session: aiohttp.ClientSession, semaphore_limit: int):
        self.url: str = url
        self.session: aiohttp.ClientSession = session
        self.semaphore: asyncio.Semaphore = asyncio.Semaphore(semaphore_limit)


    async def post(self, payload: dict):
        async with self.session.post(
                self.url,
                json=payload,
                headers=TinkoffRequest.headers,
                timeout=600
        ) as response:
            # logger.info(str(self.semaphore))
            logger.info(str(response.status))
            logger.info(payload)
            if response.ok:
                try:
                    response_body = await response.json()
                    await asyncio.sleep(60)
                except aiohttp.client_exceptions.ClientConnectorError as e:
                    logger.exception(str(e))
                    sleep(10)
                    raise ConnectionError("Restart")
                except Exception as e:
                    logger.exception(str(e))
                    sleep(60)
                    raise Exception("Restart")
                finally:
                    return response_body
            else:
                await asyncio.sleep(60)
                return []
