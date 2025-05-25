from datalake.base.meta.meta_job import ExtractMeta
from datalake.base import config
from datalake.base.utils.str import StringUtils
from datalake.base.utils.logger import Logger
from datalake.base.enums.extract import ExtractType
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar
from pyspark.sql import SparkSession

LOGGER = Logger.get_logger(__name__)


class Extract(object):
    def __init__(
        self, spark: SparkSession, extract_meta: ExtractMeta, args: dict = None
    ):
        if args is None:
            args = {}
        self.spark = spark
        self.extract_meta = extract_meta
        self.start_time = args.get("start_time")
        self.table_info = args.get("table_info")
        self.job_name = args.get("job_name")
        self.from_date = args.get("from_date")
        self.to_date = args.get("to_date")
        self.CRITERIA_FORMAT = config.DATE_FORMAT_DEFAULT
        self.CRITERIA_DTYPE = None

    def process(self):
        pass

    def get_path(self):
        path = ""
        if not StringUtils.isblank(self.extract_meta.source_zone):
            path += self.extract_meta.source_zone + "/"
        if not StringUtils.isblank(self.extract_meta.source_database):
            path += self.extract_meta.source_database + "/"
        if not StringUtils.isblank(self.extract_meta.source_schema):
            path += self.extract_meta.source_schema + "/"
        if not StringUtils.isblank(self.extract_meta.source_object):
            path += self.extract_meta.source_object
        return path

    def get_date_filter(self):
        match self.extract_meta.type:
            case ExtractType.FULL_LOAD:
                self.from_date = None
                self.to_date = None
            case ExtractType.INCREMENTAL:
                if self.from_date is not None:
                    self.from_date = self.from_date + timedelta(microseconds=1)
                else:
                    self.from_date = datetime.min
            case ExtractType.DAILY:
                day = self.to_date - timedelta(int(self.extract_meta.criteria_value))
                self.from_date = datetime.combine(day, datetime.min.time)
                self.to_date = datetime.combine(day, datetime.max.time)
            case ExtractType.MONTHLY:
                target_date = self.to_date + relativedelta(
                    months=int(self.extract_meta.criteria_value)
                )
                self.from_date = target_date.replace(
                    day=1, hour=0, minute=0, second=0, microsecond=0
                )
                last_day = calendar.monthrange(target_date.year, target_date.month)[1]
                self.to_date = target_date.replace(
                    day=last_day, hour=23, minute=59, second=59, microsecond=999999
                )
            case ExtractType.WEEKLY:
                target_date = self.to_date + relativedelta(
                    weeks=int(self.extract_meta.criteria_value)
                )
                start_of_week = target_date - timedelta(days=target_date.weekday())
                self.from_date = start_of_week.replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                self.to_date = self.from_date + timedelta(
                    days=6, hours=23, minutes=59, seconds=59, microseconds=999999
                )
            case ExtractType.YEARLY:
                target_date = self.to_date + relativedelta(
                    years=int(self.extract_meta.criteria_value)
                )
                self.from_date = target_date.replace(
                    month=1, day=1, hour=0, minute=0, second=0, microsecond=0
                )
                self.to_date = target_date.replace(
                    month=12, day=31, hour=23, minute=59, second=59, microsecond=999999
                )
