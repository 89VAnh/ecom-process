from datalake.base.enums.base_enum import BaseEnum


class SourceType(BaseEnum):
    DATABASE = "DATABASE"
    S3 = "S3"

class SourceSubType(BaseEnum):
    CSV = "CSV"
    JSON = "JSON"
    EXCEL = "EXCEL"
    ORACLE = "ORACLE"
    MSSQL = "MSSQL"
    PARQUET = "PARQUET"
    ICEBERG = "ICEBERG"