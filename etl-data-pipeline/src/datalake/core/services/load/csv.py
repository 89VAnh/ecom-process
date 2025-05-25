from datalake.core.services.load import Load
from datalake.base.utils.logger import Logger

LOGGER = Logger.get_logger("CSVLoadImpl")


class CSVLoadImpl(Load):
    def process(self):
        self.df.write.format("csv").mode(self.load_meta.type.value).save(
            self.get_path()
        )
