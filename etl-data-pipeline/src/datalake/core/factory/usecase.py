from datalake.core.session.job import JobSession
from datalake.base.enums.env import Env
from datalake.base.utils.logger import Logger
from datalake.base.utils.str import StringUtils
from datalake.core.usecases import Usecase

LOGGER = Logger.get_logger("USFactory")


class USFactory:
    @staticmethod
    def run(env: Env = Env.DEV, job_name: str = None):
        LOGGER.info(f"[run] init job session env={env.name}")
        session = JobSession(env=env)
        if not StringUtils.isblank(job_name):
            session.name = job_name
        LOGGER.info("[run] init use case")
        us = Usecase.init(session)
        LOGGER.info("[run] processing use case logic")
        us.process()
        LOGGER.info("[run] done!")
