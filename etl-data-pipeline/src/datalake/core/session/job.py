from datetime import datetime
from datalake.base import config
from datalake.base.enums.env import Env
from datalake.base.enums.spark import SparkEnv
from datalake.base.enums.usecase import USType
from datalake.base.utils.data import DataUtils
from datalake.base.utils.date import DateUtils
from datalake.base.utils.logger import Logger

LOGGER = Logger.get_logger("JobSession")


class JobSession:
    run_id: str
    name: str
    type: USType
    run_time: datetime
    completed_time: datetime
    env: Env
    warehouse_path: str
    responses: list

    def __init__(self, env: Env = Env.DEV, args_options: list = None):
        self.env = env
        args = self.get_args(env, args_options)
        self.run_time = DateUtils.now()
        self.name = DataUtils.get_dict_value(args, "JOB_NAME")
        self.run_id = DataUtils.get_dict_value(args, "JOB_RUN_ID")
        self.type = USType.value_of(DataUtils.get_dict_value(args, "JOB_TYPE"))
        self.responses = []

    def get_args(self, env: Env, args_options: list = None):
        spark_env = SparkEnv.get_env(env)
        LOGGER.debug("[get_args] SPARK ENV = %s", spark_env)
        match spark_env:
            case SparkEnv.GLUE:
                from awsglue.utils import getResolvedOptions
                import sys

                # if args_options:
                #     args = getResolvedOptions(sys.argv, args_options)
                # else:
                #     args = getResolvedOptions(sys.argv, config.JOB_DEFAULT_PARAMS)

                # return args
                return {
                    "JOB_NAME": "demo",
                    "JOB_RUN_ID": "xyz",
                    "JOB_TYPE": "INGESTION",
                }
            case SparkEnv.LOCAL:
                return {
                    "JOB_NAME": "demo",
                    "JOB_RUN_ID": "xyz",
                    "JOB_TYPE": "INGESTION",
                }
            case _:
                raise Exception(f"[get_args] Don't spark_env={spark_env} env={env}")
