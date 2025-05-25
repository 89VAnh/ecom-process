from datalake.base.enums.base_enum import BaseEnum


class USType(BaseEnum):
    DECRYPTION = "DECRYPTION"
    ENCRYPTION = "ENCRYPTION"
    INGESTION = "INGESTION"
    SOURCING = "SOURCING"
    LANDING = "LANDING"
    WAREHOUSING = "WAREHOUSING"
