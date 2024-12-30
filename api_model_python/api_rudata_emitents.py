import os
import sys

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text

from clients.RuData.RuDataDF import RuDataDF
from clients.RuData.RuDataRequest import RuDataRequest
from src.path import get_project_root, Path
from logs.Logger import Logger


logger = Logger()

def api_rudata_emitents():
    env_path: Path = Path.joinpath(get_project_root(), '.env')
    RuDataRequest.set_headers()
    Emitents = RuDataDF("Emitents").df
    if not Emitents.empty:
        load_dotenv(env_path)
        DATABASE_URI: str = (f"postgresql://"
                        f"{os.environ['POSTGRES_USER']}:"
                        f"{os.environ['POSTGRES_PASSWORD']}@"
                        f"{os.environ['POSTGRES_HOST']}:"
                        f"{os.environ['POSTGRES_PORT']}/"
                        f"{os.environ['POSTGRES_DATABASE']}")
        engine = create_engine(DATABASE_URI)
        engine.execute(sa_text(f'''TRUNCATE TABLE api_rudata_emitents''').execution_options(autocommit=True))
        Emitents.to_sql(name='api_rudata_emitents', con=engine, if_exists='append', index=False)
        logger.info(f"api_rudata_emitents downloaded successfully. rows: {str(Emitents.shape[0])}")
    else:
        logger.exception("Emitents empty")
        sys.exit(1)
