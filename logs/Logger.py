import logging
import sys
from datetime import datetime as dt
from functools import wraps

from pandas import DataFrame


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
            logging.basicConfig(
                filename=f'./logs/logger_{dt.now().strftime("%Y%m%d%H%M%S")}.log',
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

    @classmethod
    def init_logger(cls, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                cls._instance.info("Getting token started")
                result = func(*args, **kwargs)
                cls._instance.info("Getting token finished")
                return result
            except Exception as e:
                cls._instance.exception(msg=f"Getting token failed.\nException: {str(e)}")
                sys.exit(0)
        return wrapper

    @classmethod
    def logDF(cls, func):
        """
        This function is for RuData client
        Записывает начало и конец посылания запросов для определенного адреса
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                cls._instance.info(f"{args[0].key} started")
                result: DataFrame = func(*args, **kwargs)
                cls._instance.info(f"{args[0].key} finished. {args[0].key} shape {result.shape}")
                return result
            except Exception as e:
                cls._instance.exception(msg=f"Exception raised in {args[0].key}.\n{str(e)}\nRetry\n")
        return wrapper
