import asyncio
import pandas as pd
from datetime import datetime, timezone, timedelta
from grpc.aio import AioRpcError
from time import sleep
from typing import List, Optional
from tinkoff.invest import RequestError, CandleInterval, AioRequestError
from tinkoff.invest.retrying.aio.client import AsyncRetryingClient
from tinkoff.invest.retrying.settings import RetryClientSettings

from .Tbank import Tbank, logger
from .TbankAsyncServices import add_figi, HistoricCandleWithFigi


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
                        instrument_id="",
                        candle_source_type=None,
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
            from_: datetime = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
            to: Optional[datetime] = None,
            figis: List[str] = None,
            interval: CandleInterval = CandleInterval(0),
            instrument_id="",
            candle_source_type=None,
    ) -> pd.DataFrame:
        if not figis:
            figis: List[str] = self.figis
        if not interval:
            interval: CandleInterval = self.interval
        candles_df = pd.DataFrame()
        for figi in figis:
            try:
                df = await self._get_candles_by_figi(
                    from_=from_,
                    to=to,
                    figi=figi,
                    interval=interval,
                )
                candles_df = pd.concat([candles_df, df])
            except RequestError as err:
                tracking_id = err.metadata.tracking_id if err.metadata else ""
                logger.error("Error tracking_id=%s code=%s", tracking_id, str(err.code))
                sleep(60)
        candles_df = await self._parse_response(candles_df)
        await self._finish_get_data(candles_df, f'historic_candles{str(interval)}')
        return candles_df

    def run(
            self,
            from_: datetime = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1),
            to: Optional[datetime] = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
            figis: List[str] = None,
            interval: CandleInterval = CandleInterval(0),
            instrument_id="",
            candle_source_type=None,
    ) -> pd.DataFrame:
        return asyncio.run(
            self._get_all_candles(
                from_,
                to,
                figis,
                interval,
                instrument_id,
                candle_source_type,
            )
        )


class Candles1Min(Candles):

    def __init__(self):
        super().__init__()
        self.interval: CandleInterval = CandleInterval.CANDLE_INTERVAL_1_MIN


class Candles5Min(Candles):

    def __init__(self):
        super().__init__()
        self.interval: CandleInterval = CandleInterval.CANDLE_INTERVAL_5_MIN


class Candles15Min(Candles):

    def __init__(self):
        super().__init__()
        self.interval: CandleInterval = CandleInterval.CANDLE_INTERVAL_15_MIN


class CandlesHour(Candles):

    def __init__(self):
        super().__init__()
        self.interval: CandleInterval = CandleInterval.CANDLE_INTERVAL_HOUR


class CandlesDay(Candles):

    def __init__(self):
        super().__init__()
        self.interval: CandleInterval = CandleInterval.CANDLE_INTERVAL_DAY
