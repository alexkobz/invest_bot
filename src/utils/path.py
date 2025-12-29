from pathlib import Path


def get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent

def get_dotenv_path() -> Path:
    return get_project_root() / '.env'
