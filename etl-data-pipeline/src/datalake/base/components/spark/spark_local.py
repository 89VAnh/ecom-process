from pyspark.sql import SparkSession

from datalake.base.components.spark.ispark import ISpark


class SparkLocal(ISpark):
    def __init__(self, job_name):
        super().__init__(job_name)

    def init_session(self):
        self.spark: SparkSession = SparkSession.builder.config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        ).getOrCreate()
        return self.spark

    def close_session(self):
        self.spark.stop()
