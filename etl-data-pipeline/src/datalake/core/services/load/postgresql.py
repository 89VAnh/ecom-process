from datalake.core.services.load import Load
from datalake.base.utils.logger import Logger

LOGGER = Logger.get_logger("PostgreSQLLoadImpl")


class PostgreSQLLoadImpl(Load):
    def process(self):
       self.df.write.format("jdbc").format("jdbc") \
        .option("url", f"{self.load_meta.connection_url}/{self.load_meta.target_database}") \
        .option("dbtable", f"{self.load_meta.target_schema}.{self.load_meta.target_object}") \
        .option("user", self.load_meta.connection_username) \
        .option("password", self.load_meta.connection_password) \
        .option("driver", self.load_meta.connection_driver) \
        .mode(self.load_meta.type.value).save()
