import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from src.utils.path import get_dotenv_path

dotenv_path: Path = get_dotenv_path()
load_dotenv(dotenv_path=dotenv_path)

def get_engine(schema: str = 'public') -> Engine:
    DATABASE_URI: str = (
        f"postgresql://"
        f"{os.environ['POSTGRES_USER']}:"
        f"{os.environ['POSTGRES_PASSWORD']}@"
        f"{os.environ['POSTGRES_HOST']}:"
        f"{os.environ['POSTGRES_PORT']}/"
        f"{os.environ['POSTGRES_DATABASE']}")
    engine: Engine = create_engine(
        DATABASE_URI,
        connect_args={"options": f"-csearch_path={schema}"}
    )
    return engine
