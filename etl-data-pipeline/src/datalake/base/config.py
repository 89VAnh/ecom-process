import logging

from datalake.base.enums.env import Env
from datalake.base.enums.spark import SparkEnv

LOG_LEVEL = logging.DEBUG
LOG_FORMAT = "%(asctime)s - %(levelname)s %(name)s - %(message)s"
LOG_DATE_FORMAT = "%m/%d/%Y %I:%M:%S %p"
TEMP_PATH = "/tmp/"
GLUE_CATALOG = "glue_catalog"
JOB_NAME_DEFAULT = "job_name"

WAREHOUSE_PATH = {
    Env.DEV: "../../data/target/",
    Env.SIT: "../../data/target/",
    Env.UAT: "../../data/target/",
    Env.PROD: "../../data/target/",
}

CRAWLER_ROLE = {
    Env.DEV: "ARN_ROLE",
    Env.SIT: "ARN_ROLE",
    Env.UAT: "ARN_ROLE",
    Env.PROD: "ARN_ROLE",
}

SPARK_ENV = SparkEnv.GLUE

DATE_FORMAT_DEFAULT = "yyyy-MM-dd"

JOB_DEFAULT_PARAMS = ["JOB_NAME", "JOB_TYPE"]
