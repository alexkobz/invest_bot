import os

from airflow.exceptions import AirflowSkipException
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text

from src.sources.RuData.RuDataDF import RuDataDF
from api_model_python import RuDataRequest
from api_model_python import get_project_root, Path
from logs.Logger import Logger


logger = Logger()

def get_emitents():
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
        engine.execute(sa_text(f'''TRUNCATE TABLE rudata_emitents''').execution_options(autocommit=True))
        Emitents.to_sql(name='rudata_emitents', con=engine, if_exists='append', index=False)
        logger.info(f"rudata_emitents downloaded successfully. rows: {str(Emitents.shape[0])}")
    else:
        logger.exception("Emitents empty")
        raise AirflowSkipException("Skipping this task as DataFrame is empty")
