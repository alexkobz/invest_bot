import asyncio

import pandas as pd
from tinkoff.invest import RequestError
from tinkoff.invest.retrying.aio.client import AsyncRetryingClient
from tinkoff.invest.retrying.settings import RetryClientSettings

from .Tbank import Tbank, TbankStageSaver, logger


class Shares(Tbank):

    async def _get_shares(self) -> pd.DataFrame:
        async with AsyncRetryingClient(
            self.TOKEN,
            settings=RetryClientSettings(use_retry=True, max_retry_attempt=2)
        ) as client:
            try:
                shares_res = await client.instruments.shares()
            except RequestError as err:
                tracking_id = err.metadata.tracking_id if err.metadata else ""
                logger.error("Error tracking_id=%s code=%s", tracking_id, str(err.code))
        df = pd.DataFrame(shares_res.instruments)
        shares = await self._parse_response(df)
        return shares

    @TbankStageSaver(table_name='shares')
    def run(self) -> pd.DataFrame:
        return asyncio.run(self._get_shares())


class Bonds(Tbank):

    async def _get_bonds(self) -> pd.DataFrame:
        async with AsyncRetryingClient(
            self.TOKEN,
            settings=RetryClientSettings(use_retry=True, max_retry_attempt=2)
        ) as client:
            try:
                bonds_res = await client.instruments.bonds()
            except RequestError as err:
                tracking_id = err.metadata.tracking_id if err.metadata else ""
                logger.error("Error tracking_id=%s code=%s", tracking_id, str(err.code))
        df = pd.DataFrame(bonds_res.instruments)
        bonds = await self._parse_response(df)
        return bonds

    @TbankStageSaver(table_name='bonds')
    def run(self) -> pd.DataFrame:
        return asyncio.run(self._get_bonds())


class Etfs(Tbank):

    async def _get_etfs(self) -> pd.DataFrame:
        async with AsyncRetryingClient(
            self.TOKEN,
            settings=RetryClientSettings(use_retry=True, max_retry_attempt=2)
        ) as client:
            try:
                etfs_res = await client.instruments.etfs()
            except RequestError as err:
                tracking_id = err.metadata.tracking_id if err.metadata else ""
                logger.error("Error tracking_id=%s code=%s", tracking_id, str(err.code))
        df = pd.DataFrame(etfs_res.instruments)
        etfs = await self._parse_response(df)
        return etfs

    @TbankStageSaver(table_name='etfs')
    def run(self) -> pd.DataFrame:
        return asyncio.run(self._get_etfs())
