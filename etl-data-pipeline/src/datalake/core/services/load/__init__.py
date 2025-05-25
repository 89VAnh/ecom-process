from datalake.base.meta.meta_job import LoadMeta
from datalake.base.utils.str import StringUtils
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


class Load:

    def __init__(self, spark: SparkSession, dataframe: DataFrame, load_meta: LoadMeta):
        self.spark = spark
        self.df = dataframe
        self.load_meta = load_meta

    def process(self):
        pass

    def get_path(self):
        path = ""
        if not StringUtils.isblank(self.load_meta.target_zone):
            path += self.load_meta.target_zone + "/"
        if not StringUtils.isblank(self.load_meta.target_database):
            path += self.load_meta.target_database + "/"
        if not StringUtils.isblank(self.load_meta.target_schema):
            path += self.load_meta.target_schema + "/"
        if not StringUtils.isblank(self.load_meta.target_object):
            path += self.load_meta.target_object
        return path
