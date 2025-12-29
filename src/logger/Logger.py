import logging
import sys
from datetime import datetime as dt

from ..utils.path import Path, get_project_root


class Logger(logging.Logger):
    """
    Класс для логирования
    """
    _instance: logging.Logger = None

    def __new__(cls) -> logging.Logger:
        if cls._instance is not None:
            return cls._instance
        else:
            cls._instance = object.__new__(cls)
            filename = Path(get_project_root() / 'logs' / '{}.log'.format(dt.now().strftime("%Y%m%d%H%M%S")))
            logging.basicConfig(
                filename=filename,
                filemode='a+',
                level=logging.INFO,
                format='%(process)d - %(asctime)s - %(levelname)s - %(message)s'
            )
            cls._instance = logging.getLogger()
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(logging.INFO)
            cls._instance.addHandler(handler)
            cls._instance.__initialized = False
            return cls._instance
