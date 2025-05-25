from datalake.core.services.load import Load
from datalake.base.utils.logger import Logger

LOGGER = Logger.get_logger("ParquetLoadImpl")


class ParquetLoadImpl(Load):
    def process(self):
        writer = self.df.write.format("parquet").mode(self.load_meta.type.value)

        if self.load_meta.target_partition_cols:
            writer = writer.partitionBy(self.load_meta.target_partition_cols)

        writer.save(self.get_path())
