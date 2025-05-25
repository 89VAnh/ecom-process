from datalake.base import config
from datalake.base.components.metadata.imetadata import IMetadata
from datalake.base.utils.logger import Logger
from datalake.base.components.spark.ispark import ISpark

LOGGER = Logger.get_logger("SparkSession")


class SparkSession:
    def __init__(self, metadata: IMetadata, spark_config=None, args_option: list = []):
        self.args = None
        self.crawler_role = None
        self.env = metadata.session.env
        self.job_name = metadata.job_name
        self.spark = None
        self.warehouse_path = ""
        self.init_args(args_option)
        self.init_spark(spark_config)

    def init_args(self, args_option=None):
        self.warehouse_path = config.WAREHOUSE_PATH.get(self.env)
        self.crawler_role = config.CRAWLER_ROLE.get(self.env)

    def init_spark(self, spark_config: dict = None):
        spark = ISpark.init(self.env, self.job_name)
        self.spark = spark.init_session()

    def init_metadata(self):
        pass

    def close(self):
        self.spark.stop()
