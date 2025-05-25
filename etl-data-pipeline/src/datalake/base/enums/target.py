from datalake.base.enums.base_enum import BaseEnum


class TargetType(BaseEnum):
    DATABASE = "DATABASE"
    S3 = "S3"


class TargetSubType(BaseEnum):
    CSV = "CSV"
    EXCEL = "EXCEL"
    ORACLE = "ORACLE"
    MSSQL = "MSSQL"
    PARQUET = "PARQUET"
    POSTGRESQL = "POSTGRESQL"
    ICEBERG = "ICEBERG"
