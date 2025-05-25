import json

from datalake.base.enums.extract import ExtractType
from datalake.base.enums.source import SourceType, SourceSubType
from datalake.base.utils.json import JsonUtils
from datalake.base.utils.logger import Logger

LOGGER = Logger.get_logger(__name__)

"""
    @Extract Source Meta Data
"""


class HistoryMeta:
    job_name: str = None
    source_object: str = None
    source_schema: str = None
    source_database: str = None
    criteria_value: str = None
    criteria_dtype: str = None
    target_object: str = None
    target_schema: str = None
    target_database: str = None
    status: str = None
    message: str = None

    def __init__(self, hist_meta: dict = None):
        if hist_meta is not None:
            extract_flat = JsonUtils.flatten_json(hist_meta)
            for key, value in extract_flat.items():
                self.setattr(key, value)

    def setattr(self, key, value):
        try:
            if key == "type":
                value = ExtractType.valueOf(value)
            if key == "source_type":
                value = SourceType.valueOf(value)
            if key == "source_sub_type":
                value = SourceSubType.valueOf(value)
            self.__setattr__(key, value)
        except Exception as ex:
            LOGGER.debug("[setattr] key=%s value=%s msg=%s", key, value, ex)

    def __str__(self):
        return json.dumps(self.__dict__)
