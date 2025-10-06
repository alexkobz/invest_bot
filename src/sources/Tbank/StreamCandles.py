import asyncio
from dataclasses import dataclass, asdict
from time import time
from typing import List

import pandas as pd
from kafka import KafkaConsumer
from tinkoff.invest import CandleInterval, AsyncClient, CandleInstrument, SubscriptionInterval
from tinkoff.invest.market_data_stream.async_market_data_stream_manager import AsyncMarketDataStreamManager

from .Tbank import Tbank, logger
from src.kafka.tbank.candles_1min.consumer import consumer as consumer_candles_1min
from src.kafka.tbank.candles_5min.consumer import consumer as consumer_candles_5min
from ...kafka.tbank.candles_15min.consumer import consumer as consumer_candles_15min
from ...kafka.tbank.candles_hour.consumer import consumer as consumer_candles_hour
from ...kafka.tbank.candles_day.consumer import consumer as consumer_candles_day
from src.kafka.tbank.producer import producer
from src.kafka.tbank.topics import Topic


class StreamCandles(Tbank):

    def __init__(self):
        super().__init__()
        self.topic: str = None
        self.consumer: KafkaConsumer = None
        self.interval: SubscriptionInterval = None

    async def _get_all_candles(
        self,
        figis: list[str] = None,
    ) -> None:
        """
        https://github.com/Tinkoff/invest-python/blob/main/examples/easy_async_stream_client.py
        """
        if not figis:
            figis = self.figis
        num_instruments = len(figis)
        sleep_time = 30 / num_instruments
        logger.info(f'num_instruments {num_instruments}')

        async with AsyncClient(self.TOKEN) as client:
            market_data_stream: AsyncMarketDataStreamManager = (
                client.create_market_data_stream()
            )
            market_data_stream.candles.waiting_close().subscribe(
                [
                    CandleInstrument(
                        figi=figi,
                        interval=self.interval,
                    ) for figi in figis
                ]
            )

            while True:
                async for marketdata in market_data_stream:
                    producer.send(topic=self.topic, value=asdict(marketdata))
                    producer.flush()
                    await asyncio.sleep(sleep_time)

    async def _consume_all_candles(self):
        buffer = []
        last_flush = time()

        async def flush_buffer():
            nonlocal buffer, last_flush
            if buffer:
                df = pd.DataFrame(buffer)
                df = await self._parse_response(df)
                df.to_sql(
                    name=self.topic,
                    con=self.engine,
                    if_exists='append',
                    index=False)
                buffer = []
                last_flush = time()

        for msg in self.consumer:
            # if msg.value.get("subscribe_candles_response") is not None:
                # num_instruments = len(msg.value["subscribe_candles_response"]['candles_subscriptions'])
            if msg.value.get("candle") is not None:
                buffer.append(msg.value["candle"])
                if time() - last_flush >= 60:
                    await flush_buffer()


    def consume_all_candles(
        self
    ) -> None:
        asyncio.run(self._consume_all_candles())

    def run(
        self,
        figis: List[str] = None,
    ) -> None:
        asyncio.run(self._get_all_candles(figis))


class StreamCandles1Min(StreamCandles):
    def __init__(self):
        super().__init__()
        self.topic: str = Topic.CANDLES1MIN.value
        self.consumer: KafkaConsumer = consumer_candles_1min
        self.interval: SubscriptionInterval = SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE


class StreamCandles5Min(StreamCandles):
    def __init__(self):
        super().__init__()
        self.topic: str = Topic.CANDLES5MIN.value
        self.consumer: KafkaConsumer = consumer_candles_5min
        self.interval: SubscriptionInterval = SubscriptionInterval.SUBSCRIPTION_INTERVAL_FIVE_MINUTES


class StreamCandles15Min(StreamCandles):
    def __init__(self):
        super().__init__()
        self.topic: str = Topic.CANDLES15MIN.value
        self.consumer: KafkaConsumer = consumer_candles_15min
        self.interval: SubscriptionInterval = SubscriptionInterval.SUBSCRIPTION_INTERVAL_FIFTEEN_MINUTES


class StreamCandlesHour(StreamCandles):
    def __init__(self):
        super().__init__()
        self.topic: str = Topic.CANDLESHOUR.value
        self.consumer: KafkaConsumer = consumer_candles_hour
        self.interval: SubscriptionInterval = SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_HOUR


class StreamCandlesDay(StreamCandles):
    def __init__(self):
        super().__init__()
        self.topic: str = Topic.CANDLESDAY.value
        self.consumer: KafkaConsumer = consumer_candles_day
        self.interval: SubscriptionInterval = SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_DAY
