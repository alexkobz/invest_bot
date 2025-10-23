from enum import Enum


class Topic(Enum):
    CANDLES1MIN = 'candles_1min'
    CANDLES5MIN = 'candles_5min'
    CANDLES15MIN = 'candles_15min'
    CANDLESHOUR = 'candles_hour'
    CANDLESDAY = 'candles_day'
