from __future__ import annotations

import asyncio
import socket
from time import sleep
from typing import Any, Dict, List

import aiohttp
import pandas as pd

from src.logger.Logger import Logger
from src.postgres.StageSaver import StageSaver
from src.sources.Rudata.RuData import RuDataStrategy
from src.utils.retries import retry

SCHEMA: str = 'rudata'
LIMIT: int = 5

logger = Logger()


class RuDataStageSaver(StageSaver):
    def __init__(self, **kwargs):
        super().__init__(schema=SCHEMA, **kwargs)


class RuDataDF(RuDataStrategy):

    headers: Dict[str, str] = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/39.0.2171.95 Safari/537.36',
        'content-type': 'application/json',
        'Accept': 'application/json',
    }
    semaphore: asyncio.Semaphore = asyncio.Semaphore(LIMIT)

    def __init__(self):
        self._list_json: List[dict] = []
        self._df: pd.DataFrame = pd.DataFrame()

    @classmethod
    def set_headers(cls, headers: dict):
        cls.headers.update(headers)

    def run(self) -> pd.DataFrame:
        if 'Authorization' not in self.headers or self.headers['Authorization'] is None or self.headers['Authorization'] == 'Bearer ':
            raise ValueError("Authorization header is not set. Run Account()")
        df: pd.DataFrame = asyncio.run(self.send_requests())
        return df

    @property
    def df(self) -> pd.DataFrame:
        df: pd.DataFrame = self.run()
        self._df = df
        return self._df

    @df.setter
    def df(self, value) -> None:
        self._df = value

    def get_df(self):
        return self.df

    def payloads(self):
        raise NotImplemented

    def create_tasks(self, chunk_payloads: List[dict], session: aiohttp.ClientSession) -> List[asyncio.Task]:
        return [asyncio.create_task(
            self.post(session=session, payload=payload)
            ) for payload in chunk_payloads]

    @staticmethod
    async def safe_task(task, timeout=120) -> List[Any]:
        try:
            return await asyncio.wait_for(task, timeout=timeout)
        except asyncio.TimeoutError:
            return []

    @retry(
        exceptions=(TimeoutError, ConnectionError, Exception),
        tries=5,
        delay=100,
        logger=logger
    )
    async def execute_tasks(self, tasks: List[asyncio.Task]) -> bool:
        resAll: List[List[dict]] = await asyncio.gather(*(self.safe_task(t, 60) for t in tasks))
        result: List[dict] = [row for task_res in resAll for row in task_res]
        self._list_json.extend(result)
        logger.info(f"Chunk done {len(result)}" )
        return bool(result)

    @retry(
        exceptions=(TimeoutError, ConnectionError, Exception),
        tries=3,
        delay=600,
        logger=logger
    )
    async def send_requests(self) -> pd.DataFrame:
        async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=LIMIT,
                                               family=socket.AF_INET),
                trust_env=True,
                timeout=aiohttp.ClientTimeout(7200)
        ) as session:

            for chunk_payloads in self.payloads():
                tasks: List[asyncio.Task] = self.create_tasks(chunk_payloads, session)
                if tasks:
                    code: bool = await self.execute_tasks(tasks)
                    if not code and isinstance(self, RuDataPagesDF):
                        return pd.DataFrame(self._list_json)
            return pd.DataFrame(self._list_json)

    async def post(self, session, payload):
        async with self.semaphore, session.post(
                self.url,
                json=payload,
                headers=self.headers,
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
                    await self.post(session, payload)
                except Exception as e:
                    logger.exception(str(e))
                    sleep(60)
                    raise Exception("Restart")
                finally:
                    return response_body
            else:
                return []

class RuDataPagesDF(RuDataDF):
    pass
