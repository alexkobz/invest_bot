import asyncio
from datetime import datetime, timezone
from time import sleep
from typing import List

import pandas as pd
from tinkoff.invest import RequestError, CandleInterval
from tinkoff.invest.retrying.aio.client import AsyncRetryingClient
from tinkoff.invest.retrying.settings import RetryClientSettings

from .Tbank import Tbank, logger
from .TbankAsyncServices import add_figi, HistoricCandleWithFigi


class Candles(Tbank):

    def __init__(self):
        super().__init__()
        self.interval: CandleInterval = None
        self.from_: datetime = None

    async def _get_candles_by_figi(
            self,
            figi: str = '',
            from_: datetime = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
            interval: CandleInterval = None,
    ) -> pd.DataFrame:
        async with AsyncRetryingClient(
                self.TOKEN,
                settings=RetryClientSettings(use_retry=True, max_retry_attempt=2)
        ) as client:
            candles: List[HistoricCandleWithFigi] = []
            try:
                async for candle in client.get_all_candles(
                        figi=figi,
                        from_=from_,
                        interval=interval,
                ):
                    candle_with_figi = add_figi(candle, figi)
                    candles.append(candle_with_figi)
            except RequestError as err:
                tracking_id = err.metadata.tracking_id if err.metadata else ""
                logger.error("Error tracking_id=%s code=%s", tracking_id, str(err.code))
            candles_df: pd.DataFrame = pd.DataFrame(candles)
            return candles_df

    async def _get_all_candles(
            self,
            from_: datetime = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
            figis: List[str] = None,
    ) -> pd.DataFrame:
        if not figis:
            figis: List[str] = self.figis
        candles_df = pd.DataFrame()
        for figi in figis:
            try:
                df = await self._get_candles_by_figi(
                    figi,
                    from_=from_ if from_ else self.from_,
                    interval=self.interval,
                )
                candles_df = pd.concat([candles_df, df])
            except RequestError as err:
                tracking_id = err.metadata.tracking_id if err.metadata else ""
                logger.error("Error tracking_id=%s code=%s", tracking_id, str(err.code))
                sleep(60)
        candles_df = await self._parse_response(candles_df)
        await self._finish_get_data(candles_df, 'historic_candles')
        return candles_df

    def run(
            self,
            from_: datetime = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
            figis: List[str] = None,
    ) -> pd.DataFrame:
        return asyncio.run(self._get_all_candles(from_, figis))


class Candles1Min(Candles):

    def __init__(self, from_: datetime = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)):
        super().__init__()
        self.interval: CandleInterval = CandleInterval.CANDLE_INTERVAL_1_MIN
        self.from_: datetime = from_


class Candles5Min(Candles):

    def __init__(self, from_: datetime = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)):
        super().__init__()
        self.interval: CandleInterval = CandleInterval.CANDLE_INTERVAL_5_MIN
        self.from_: datetime = from_


class Candles15Min(Candles):

    def __init__(self, from_: datetime = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)):
        super().__init__()
        self.interval: CandleInterval = CandleInterval.CANDLE_INTERVAL_15_MIN
        self.from_: datetime = from_


class CandlesHour(Candles):

    def __init__(self, from_: datetime = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)):
        super().__init__()
        self.interval: CandleInterval = CandleInterval.CANDLE_INTERVAL_HOUR
        self.from_: datetime = from_


class CandlesDay(Candles):

    def __init__(self, from_: datetime = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)):
        super().__init__()
        self.interval: CandleInterval = CandleInterval.CANDLE_INTERVAL_DAY
        self.from_: datetime = from_
