from datalake.base.enums.env import Env
from datalake.base.enums.spark import SparkEnv


class ISpark:
    def __init__(self, job_name):
        self.job_name = job_name
        self.config = None
        self.spark = None
        self.args = {}

    def init_session(self):
        pass

    def close_session(self):
        pass

    @staticmethod
    def init(env: Env, job_name: str):
        match SparkEnv.get_env(env):
            case SparkEnv.LOCAL:
                from datalake.base.components.spark.spark_local import SparkLocal

                return SparkLocal(job_name)
            case SparkEnv.GLUE:
                from datalake.base.components.spark.spark_glue import SparkGlue

                return SparkGlue(job_name)
            case _:
                return ISpark(job_name)
