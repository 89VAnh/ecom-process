from datalake.base.enums.base_enum import BaseEnum


class Env(BaseEnum):
    DEV = "Development"
    SIT = "System Integration Testing"
    UAT = "User Acceptance Testing"
    PROD = "Production"
    STAGING = "Staging"
    TEST = "Testing"
