import re
from datalake.base.meta.meta_job import ExtractMeta
from datalake.base.utils.logger import Logger
from datalake.base.utils.str import StringUtils
from datalake.base.utils.date import DateUtils
from datetime import datetime
import boto3
import io
from pyspark.sql.functions import lit

LOGGER = Logger.get_logger(__name__)


class DataUtils:
    @staticmethod
    def get_dict_value(args: dict, key: str):
        if key in args.keys():
            return args[key]
        else:
            return None

    @staticmethod
    def align_and_union(df1, df2):
        # Add missing columns to df1
        for col in set(df2.columns) - set(df1.columns):
            df1 = df1.withColumn(col, lit(None))

        # Add missing columns to df2
        for col in set(df1.columns) - set(df2.columns):
            df2 = df2.withColumn(col, lit(None))

        # Ensure column order is the same before union
        return df1.select(sorted(df1.columns)).unionByName(
            df2.select(sorted(df2.columns))
        )

    @staticmethod
    def get_list_objects(
        extract_meta: ExtractMeta, from_date: datetime = None, to_date: datetime = None
    ):
        is_get_all = from_date is None and to_date is None
        if from_date is not None:
            from_date = DateUtils.to_utc(from_date)
        if to_date is not None:
            to_date = DateUtils.to_utc(to_date)
        s3_client = boto3.client("s3")
        paginator = s3_client.get_paginator("list_objects_v2")

        prefix = StringUtils.build_string(
            extract_meta.source_database,
            extract_meta.source_schema,
            extract_meta.source_object,
            separator="/",
        )
        object_pattern = r".*/\d{14}.json$"
        LOGGER.debug("file prefix : %s", prefix)
        pages = paginator.paginate(Bucket=extract_meta.source_zone, Prefix=prefix)
        paths = []
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    LOGGER.debug("object: %s", obj)
                    if re.match(object_pattern, obj["Key"]):
                        if is_get_all:
                            paths.append(
                                f"s3a://{extract_meta.source_zone}/{obj['Key']}"
                            )
                            continue
                        if from_date <= obj["LastModified"] <= to_date:
                            paths.append(
                                f"s3a://{extract_meta.source_zone}/{obj['Key']}"
                            )
        return paths

    @staticmethod
    def read_s3_object(bucket: str, obj_key: str):
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=obj_key)
        return io.BytesIO(obj["Body"].read())

    @staticmethod
    def write_s3_object(bucket: str, object_key: str, data):
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=bucket,
            Key=object_key,
            Body=data,
            ContentType="application/octet-stream",
        )
