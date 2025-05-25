from datalake.base.enums.base_enum import BaseEnum
from datalake.base.enums.env import Env


class SparkEnv(BaseEnum):
    GLUE = "Glue"
    LOCAL = "Local"
    PROTO = "Databricks Proto"

    @staticmethod
    def get_env(env: Env):
        spark_env = SparkEnv.GLUE
        if env == Env.DEV:
            # spark_env = SparkEnv.LOCAL
            spark_env = SparkEnv.GLUE
        return spark_env
