import logging

from datalake.base import config


class Logger:
    @staticmethod
    def get_logger(name: str = "ROOT"):
        logging.basicConfig(format=config.LOG_FORMAT, datefmt=config.LOG_DATE_FORMAT)
        logger = logging.getLogger(name)
        logger.setLevel(config.LOG_LEVEL)
        return logger
