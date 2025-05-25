from datalake.core.services.load import Load
from datalake.base.utils.logger import Logger

LOGGER = Logger.get_logger("IcebergLoad")


class IcebergLoad(Load):
    def process(self):
        table = f"glue_catalog.{self.load_meta.target_database}.{self.load_meta.target_object}"
        path = f"{self.load_meta.target_zone}/{self.load_meta.target_database}/{self.load_meta.target_object}"

        write_builder = self.df.write.mode(self.load_meta.type.value).option(
            "path", path
        )

        if "partition_date" in self.df.columns:
            write_builder = write_builder.partitionBy("partition_date")

        write_builder.saveAsTable(table)
