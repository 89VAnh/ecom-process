from enum import Enum
from datalake.base.utils.str import StringUtils


class BaseEnum(Enum):
    @classmethod
    def value_of(cls, s: str):
        try:
            return cls[StringUtils.upper(s)]
        except KeyError:
            return None