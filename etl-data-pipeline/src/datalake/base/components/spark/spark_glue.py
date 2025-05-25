from datalake.base.components.spark.ispark import ISpark
from pyspark import SQLContext
from pyspark.sql import SparkSession


from awsglue.context import GlueContext
from awsglue.job import Job


class SparkGlue(ISpark):
    def __init__(self, job_name):
        super().__init__(job_name)

    def init_session(self):
        self.spark = (
            SparkSession.builder.config(
                f"spark.sql.catalog.glue_catalog",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(
                f"spark.sql.catalog.glue_catalog.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog",
            )
            .config(
                f"spark.sql.catalog.glue_catalog.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .config(f"spark.sql.catalog.handle-timestamp-without-timezone", "true")
            .config(
                f"spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            ).config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        sc = self.spark.sparkContext
        self.sql_context = SQLContext(sc)
        self.glue_context = GlueContext(sc)
        self.job = Job(self.glue_context)
        self.job.init(job_name=self.job_name, args=self.args)
        return self.spark

    def close_session(self):
        self.spark.stop()
