from datalake.core.services.extract import Extract
from datalake.base.utils.logger import Logger
from pyspark.sql.functions import col


LOGGER = Logger.get_logger("IcebergExtract")


class IcebergExtract(Extract):

    def process(self):
        self.get_date_filter()
        table = f"glue_catalog.{self.extract_meta.source_database}.{self.extract_meta.source_object}"
        df = self.spark.table(table)
        if "crawled_at" in df.columns:
            if self.from_date is not None and self.to_date is not None:
                df = df.filter(col("crawled_at").between(self.from_date, self.to_date))
            elif self.from_date is not None:
                df = df.filter(col("crawled_at") >= self.from_date)
            elif self.to_date is not None:
                df = df.filter(col("crawled_at") <= self.to_date)
        LOGGER.debug("[process] criteria_value: %s", self.to_date)
        return {"data": df, "criteria_value": self.to_date}
