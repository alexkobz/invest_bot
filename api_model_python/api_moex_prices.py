import os
import pandas as pd

from airflow.exceptions import AirflowSkipException
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text

from clients.Moex.MoexAPI import Prices
from clients.Moex.iss_client import (Config,
                                     MicexAuth,
                                     MicexISSClientPrices)
from exceptions.MoexAuthenticationError import MoexAuthenticationError
from src.path import get_project_root, Path
from logs.Logger import Logger


logger = Logger()

def api_moex_prices():
    env_path: Path = Path.joinpath(get_project_root(), '.env')
    load_dotenv(env_path)
    my_config: Config = Config(
        user=os.environ["EMAIL_LOGIN"],
        password=os.environ["EMAIL_PASSWORD"],
        proxy_url='')
    my_auth: MicexAuth = MicexAuth(my_config)
    if my_auth.is_real_time():
        DATABASE_URI: str = (f"postgresql://"
                        f"{os.environ['POSTGRES_USER']}:"
                        f"{os.environ['POSTGRES_PASSWORD']}@"
                        f"{os.environ['POSTGRES_HOST']}:"
                        f"{os.environ['POSTGRES_PORT']}/"
                        f"{os.environ['POSTGRES_DATABASE']}")
        engine = create_engine(DATABASE_URI)
        boardids = (
            pd.read_sql(
                """
                SELECT DISTINCT boardid
                FROM public_marts.dim_moex_boards
                WHERE is_traded = true
                """
                , engine
            )['boardid']
            .to_list()
        )
        moex_prices_df: pd.DataFrame = pd.DataFrame()
        iss = MicexISSClientPrices(my_config, my_auth)
        for boardid in boardids:
            df: pd.DataFrame = iss.get_data(boardid)
            moex_prices_df = pd.concat([moex_prices_df, df], axis=0)
        if moex_prices_df.empty:
            raise AirflowSkipException("Skipping this task as DataFrame is empty")
        moex_prices_df.columns = moex_prices_df.columns.str.lower()
        engine.execute(sa_text(f'''TRUNCATE TABLE {Prices.table_name}''').execution_options(autocommit=True))
        moex_prices_df.to_sql(name=Prices.table_name, con=engine, if_exists='append', index=False)
        logger.info(f"{Prices.table_name} downloaded successfully")
    else:
        logger.info(f"{str(MoexAuthenticationError)}")
        raise MoexAuthenticationError()
