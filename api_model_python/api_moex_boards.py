import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text

from clients.Moex.MoexAPI import Boards
from clients.Moex.iss_client import (Config,
                                     MicexAuth,
                                     MicexISSClientBoards)
from exceptions.MoexAuthenticationError import MoexAuthenticationError
from src.path import get_project_root, Path
from dbt.logs.Logger import Logger


logger = Logger()

def api_moex_boards():
    load_dotenv(Path.joinpath(get_project_root(), '.env'))
    my_config = Config(user=os.environ["EMAIL_LOGIN"],
                        password=os.environ["EMAIL_PASSWORD"],
                        proxy_url='')
    my_auth = MicexAuth(my_config)
    if my_auth.is_real_time():
        DATABASE_URI = (f"postgresql://"
                        f"{os.environ['POSTGRES_USER']}:"
                        f"{os.environ['POSTGRES_PASSWORD']}@"
                        f"{os.environ['POSTGRES_HOST']}:"
                        f"{os.environ['POSTGRES_PORT']}/"
                        f"{os.environ['POSTGRES_DATABASE']}")
        engine = create_engine(DATABASE_URI)
        iss = MicexISSClientBoards(my_config, my_auth)
        df = iss.get_data(Boards)
        df.columns = df.columns.str.lstrip('@')
        engine.execute(sa_text(f'''TRUNCATE TABLE {Boards.table_name}''').execution_options(autocommit=True))
        df.to_sql(name=Boards.table_name, con=engine, if_exists='append', index=False)
        logger.info(f"{Boards.table_name} downloaded successfully")
    else:
        logger.info(f"{str(MoexAuthenticationError)}")
        raise MoexAuthenticationError()
