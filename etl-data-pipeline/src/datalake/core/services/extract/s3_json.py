import re
from datalake.core.services.extract import Extract
from datalake.base.utils.logger import Logger
from datalake.base.utils.str import StringUtils
from datalake.base.utils.data import DataUtils
from pyspark.sql.functions import lit, to_timestamp, date_format, col

LOGGER = Logger.get_logger("JSONExtract")


class JSONExtract(Extract):

    def process(self):
        self.get_date_filter()
        list_objects = DataUtils.get_list_objects(
            self.extract_meta, self.from_date, self.to_date
        )
        LOGGER.debug("this is paths: %s", list_objects)
        spark_reader = self.spark.read.format("json")
        if not StringUtils.isblank(self.extract_meta.schema):
            LOGGER.debug("[process] set schema for df: %s", self.extract_meta.schema)
            spark_reader = spark_reader.schema(self.extract_meta.schema)

        df = None
        for object in list_objects:
            try:
                LOGGER.debug("[process] path=%s", object)
                temp_df = spark_reader.load(object)
                timestamp_str = re.search(r"/(\d{14})\.json$", object).group(1)

                temp_df = temp_df.withColumn(
                    "crawled_at",
                    to_timestamp(lit(timestamp_str), "yyyyMMddHHmmss"),
                ).withColumn(
                    "partition_date",
                    date_format(col("crawled_at"), "yyyyMMdd"),
                )
                if df is None:
                    df = temp_df
                else:
                    df = DataUtils.align_and_union(df, temp_df)
            except Exception as e:
                LOGGER.debug("[process] object cause error: %s", object)
                raise e
        LOGGER.debug("[process] criteria_value: %s", self.to_date)
        return {"data": df, "criteria_value": self.to_date}
