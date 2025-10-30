from abc import ABC, abstractmethod
import pandas as pd
from airflow.exceptions import AirflowSkipException
import os
from typing import Dict, List
from dotenv import load_dotenv
from sqlalchemy import create_engine

from src.logger.Logger import Logger
from src.utils.path import Path, get_dotenv_path


SCHEMA = 'tbank'

logger = Logger()
dotenv_path: Path = get_dotenv_path()
load_dotenv(dotenv_path=dotenv_path)


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
        DATABASE_URI: str = (
            f"postgresql://"
            f"{os.environ['POSTGRES_USER']}:"
            f"{os.environ['POSTGRES_PASSWORD']}@"
            f"{os.environ['POSTGRES_HOST']}:"
            f"{os.environ['POSTGRES_PORT']}/"
            f"{os.environ['POSTGRES_DATABASE']}")
        self.engine = create_engine(
            DATABASE_URI,
            connect_args={"options": f"-csearch_path={SCHEMA}"}
        )
        self.TOKEN = os.environ["INVEST_TOKEN"]

    async def _finish_get_data(self, df: pd.DataFrame, table_name: str) -> bool:
        if df.empty:
            raise AirflowSkipException("Skipping this task as DataFrame is empty")
        try:
            try:
                self.engine.execute(f'''TRUNCATE TABLE "{table_name}"''' , autocommit=True)
            except Exception as e:
                logger.error(e)
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists='append',
                index=False)
            logger.info(f"{table_name} downloaded successfully")
            return True
        except Exception as e:
            logger.exception(f"{table_name} failed to download due to\n{e}")
            return False

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
            sql="""SELECT DISTINCT figi
                   FROM shares
                   WHERE coalesce(figi, '') != ''""",
            con=self.engine
        )['figi'].tolist()

    @abstractmethod
    def run(self) -> pd.DataFrame:
        raise NotImplementedError
