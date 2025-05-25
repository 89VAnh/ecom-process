from datalake.base.enums.base_enum import BaseEnum


class LoadType(BaseEnum):
    OVERWRITE = "OVERWRITE"
    APPEND = "APPEND"
    PARTITION_OVERWRITE = "PARTITION_OVERWRITE"
