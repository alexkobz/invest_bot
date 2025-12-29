from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import List

import pandas as pd


class RuDataStrategy(ABC):

    @abstractmethod
    def payloads(self) -> Iterable[List[dict]] | List[dict]:
        pass

    @abstractmethod
    async def send_requests(self) -> pd.DataFrame:
        pass
