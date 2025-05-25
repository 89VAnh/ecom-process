from datalake.base.enums.base_enum import BaseEnum


class ExtractType(BaseEnum):
    FULL_LOAD = "FULL_LOAD"
    INCREMENTAL = "INCREMENTAL"
    DAILY = "DAILY"
    MONTHLY = "MONTHLY"
    WEEKLY = "WEEKLY"
    YEARLY = "YEARLY"
    DATE_RANGE = "DATE_RANGE"
