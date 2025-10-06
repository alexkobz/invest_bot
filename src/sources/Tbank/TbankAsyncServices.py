from dataclasses import dataclass, asdict
from tinkoff.invest.schemas import HistoricCandle


@dataclass(eq=True, repr=True, frozen=True)
class HistoricCandleWithFigi(HistoricCandle):
    figi: str = ''

def add_figi(candle: HistoricCandle, figi: str) -> HistoricCandleWithFigi:
    return HistoricCandleWithFigi(**asdict(candle), figi=figi)
