import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from time import sleep
from typing import List, Optional

import pandas as pd
from grpc.aio import AioRpcError
from tinkoff.invest import AioRequestError, CandleInterval, RequestError
from tinkoff.invest.retrying.aio.client import AsyncRetryingClient
from tinkoff.invest.retrying.settings import RetryClientSettings
from tinkoff.invest.schemas import CandleSource

from src.sources.Tbank.Tbank import Tbank, TbankStageSaver, logger
from src.sources.Tbank.TbankAsyncServices import (HistoricCandleWithFigi,
                                                  add_figi)


@dataclass
class CandlesParameters:
    from_: datetime = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    to: Optional[datetime] = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    figis: List[str] = field(default_factory=list)
    interval: CandleInterval = CandleInterval(0)
    instrument_id: str = ""
    candle_source_type: CandleSource = None


class Candles(Tbank):

    def __init__(self):
        super().__init__()
        self.interval: CandleInterval = None

    async def _get_candles_by_figi(
        self,
        from_: datetime = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
        to: Optional[datetime] = None,
        figi: str = '',
        interval: CandleInterval = CandleInterval(0),
        instrument_id="",
        candle_source_type=None,
    ) -> pd.DataFrame:
        async with AsyncRetryingClient(
                self.TOKEN,
                settings=RetryClientSettings(use_retry=True, max_retry_attempt=2)
        ) as client:
            candles: List[HistoricCandleWithFigi] = []
            try:
                async for candle in client.get_all_candles(
                    from_=from_,
                    to=to,
                    interval=interval,
                    figi=figi,
                    instrument_id=instrument_id,
                    candle_source_type=candle_source_type,
                ):
                    candle_with_figi = add_figi(candle, figi)
                    candles.append(candle_with_figi)
            except (RequestError, AioRequestError, AioRpcError) as err:
                tracking_id = err.metadata.tracking_id if err.metadata else ""
                logger.error("Error tracking_id=%s code=%s", tracking_id, str(err.code))
                sleep(1)
                return pd.DataFrame()
            except Exception as e:
                logger.error("Error=%s", str(e))
                sleep(1)
                return pd.DataFrame()

            candles_df: pd.DataFrame = pd.DataFrame(candles)
            return candles_df

    async def _get_all_candles(
        self,
        candle: CandlesParameters = CandlesParameters(),
    ) -> pd.DataFrame:
        figis: List[str] = candle.figis or self.figis
        interval: CandleInterval = candle.interval or self.interval
        candles_df = pd.DataFrame()
        for figi in figis:
            try:
                df = await self._get_candles_by_figi(
                    from_=candle.from_,
                    to=candle.to,
                    interval=interval,
                    figi=figi,
                    instrument_id='',
                    candle_source_type=None,
                )
                candles_df = pd.concat([candles_df, df])
            except RequestError as err:
                tracking_id = err.metadata.tracking_id if err.metadata else ""
                logger.error("Error tracking_id=%s code=%s", tracking_id, str(err.code))
                sleep(60)
        candles_df = await self._parse_response(candles_df)
        return candles_df

    def run(
        self,
        candle: CandlesParameters = CandlesParameters(),
    ) -> pd.DataFrame:
        return asyncio.run(
            self._get_all_candles(candle)
        )


class Candles1MinYesterday(Candles):

    def __init__(self):
        super().__init__()
        self.interval: CandleInterval = CandleInterval.CANDLE_INTERVAL_1_MIN

    @TbankStageSaver(table_name='Candles1MinYesterday')
    def run(self, candle: CandlesParameters = CandlesParameters()) -> pd.DataFrame:
        return super().run(candle)

class Candles5MinYesterday(Candles):

    def __init__(self):
        super().__init__()
        self.interval: CandleInterval = CandleInterval.CANDLE_INTERVAL_5_MIN

    @TbankStageSaver(table_name='Candles5MinYesterday')
    def run(self, candle: CandlesParameters = CandlesParameters()) -> pd.DataFrame:
        return super().run(candle)


class Candles15MinYesterday(Candles):

    def __init__(self):
        super().__init__()
        self.interval: CandleInterval = CandleInterval.CANDLE_INTERVAL_15_MIN

    @TbankStageSaver(table_name='Candles15MinYesterday')
    def run(self, candle: CandlesParameters = CandlesParameters()) -> pd.DataFrame:
        return super().run(candle)


class CandlesHourYesterday(Candles):

    def __init__(self):
        super().__init__()
        self.interval: CandleInterval = CandleInterval.CANDLE_INTERVAL_HOUR

    @TbankStageSaver(table_name='CandlesHourYesterday')
    def run(self, candle: CandlesParameters = CandlesParameters()) -> pd.DataFrame:
        return super().run(candle)


class CandlesYesterday(Candles):

    def __init__(self):
        super().__init__()
        self.interval: CandleInterval = CandleInterval.CANDLE_INTERVAL_DAY

    @TbankStageSaver(table_name='CandlesYesterday')
    def run(self, candle: CandlesParameters = CandlesParameters()) -> pd.DataFrame:
        return super().run(candle)
