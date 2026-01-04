from enum import StrEnum
from pathlib import Path

class Mode(StrEnum):
    REPLACE = 'replace'
    APPEND = 'append'

PROJECT_DIR: Path = Path(__file__).parent
CONFIG_DIR: Path = Path(__file__)
DAGS_DIR: Path = PROJECT_DIR / 'dags'
DATA_DIR: Path = PROJECT_DIR / 'data'
DOCKER_DIR: Path = PROJECT_DIR / 'docker'
DBT_DIR: Path = PROJECT_DIR / 'dbt'
DOCS_DIR: Path = PROJECT_DIR / 'docs'
LOGS_DIR: Path = PROJECT_DIR / 'logs'
PLUGINS_DIR: Path = PROJECT_DIR / 'plugins'
SCRIPTS_DIR: Path = PROJECT_DIR / 'scripts'
SRC_DIR: Path = PROJECT_DIR / 'src'
TESTS_DIR: Path = PROJECT_DIR / 'tests'
