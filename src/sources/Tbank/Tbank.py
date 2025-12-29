import os
from abc import ABC, abstractmethod
from typing import Dict, List

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy.engine import Engine

from src.logger.Logger import Logger
from src.postgres.engine import get_engine
from src.postgres.StageSaver import StageSaver
from src.utils.path import Path, get_dotenv_path

SCHEMA: str = 'tbank'

logger = Logger()

dotenv_path: Path = get_dotenv_path()
load_dotenv(dotenv_path=dotenv_path)

engine: Engine = get_engine(schema=SCHEMA)


class TbankStageSaver(StageSaver):
    def __init__(self, **kwargs):
        super().__init__(schema=SCHEMA, **kwargs)


class Tbank(ABC):
    """
    Class for sending requests to Tbank invest and getting responses
    """

    headers: Dict[str, str] = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/39.0.2171.95 Safari/537.36',
        'content-type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'Bearer ' + os.environ["INVEST_TOKEN"]
    }

    def __init__(self):
        self.TOKEN = os.environ["INVEST_TOKEN"]

    async def _parse_response(
            self,
            df: pd.DataFrame,
    ) -> pd.DataFrame:
        dict_cols = [c for c in df.columns if df[c].apply(lambda v: isinstance(v, dict)).any()]

        # распарсим каждую колонку отдельно и добавим суффикс
        parsed_parts = []
        for col in dict_cols:
            expanded = df[col].apply(pd.Series).add_prefix(f"{col}_")
            parsed_parts.append(expanded)

        # собираем итоговый датафрейм
        df_parsed = pd.concat([df.drop(columns=dict_cols)] + parsed_parts, axis=1)
        if 'required_tests' in df_parsed.columns:
            df_parsed['required_tests'] = df_parsed['required_tests'].apply(lambda v: list(v) if v is not None else None)
        return df_parsed

    @property
    def figis(self) -> List[str]:
        return pd.read_sql(
            sql="""
                SELECT DISTINCT figi
                FROM shares
                WHERE coalesce(figi, '') != ''
            """,
            con=engine
        )['figi'].tolist()

    @abstractmethod
    def run(self, *args, **kwargs) -> pd.DataFrame:
        raise NotImplementedError
