import json

from datalake.base.enums.extract import ExtractType
from datalake.base.enums.load import LoadType
from datalake.base.enums.source import SourceType, SourceSubType
from datalake.base.enums.target import TargetSubType, TargetType
from datalake.base.utils.json import JsonUtils
from datalake.base.utils.logger import Logger

LOGGER = Logger.get_logger(__name__)

"""
    @Extract Source Meta Data
"""


class ExtractMeta:
    type: ExtractType
    params: dict
    schema: str
    source_object: str
    source_schema: str
    source_database: str
    source_zone: str
    source_type: SourceType
    source_sub_type: SourceSubType
    source_script: str
    source_path: str
    connection_name: str
    connection_url: str
    connection_host: str
    connection_port: str
    connection_database: str
    connection_username: str
    connection_password: str
    connection_driver: str
    criteria_col: str
    criteria_format: str
    criteria_value: str
    criteria_dtype: str
    limit: int
    batch: int
    size: int

    def __init__(self, extract_data: dict = None):
        if extract_data is not None:
            extract_flat = JsonUtils.flatten_json(extract_data, ["params"])
            for key, value in extract_flat.items():
                self.setattr(key, value)

    def setattr(self, key, value):
        try:
            if key == "type":
                value = ExtractType.value_of(value)
            if key == "source_type":
                value = SourceType.value_of(value)
            if key == "source_sub_type":
                value = SourceSubType.value_of(value)
            self.__setattr__(key, value)
        except Exception as ex:
            LOGGER.debug("[setattr] key=%s value=%s msg=%s", key, value, ex)

    def __str__(self):
        return json.dumps(self.__dict__)


class LoadMeta:
    type: LoadType
    params: dict
    schema: str
    target_object: str
    target_schema: str
    target_partition_cols: list[str]
    target_database: str
    target_zone: str
    target_type: TargetType
    target_sub_type: TargetSubType
    target_path: str
    connection_name: str
    connection_url: str
    connection_host: str
    connection_port: str
    connection_database: str
    connection_username: str
    connection_password: str
    connection_driver: str
    partition_col: str

    def __init__(self, load_data: dict = None):
        if load_data is not None:
            extract_flat = JsonUtils.flatten_json(load_data, ["params"])
            for key, value in extract_flat.items():
                self.setattr(key, value)

    def setattr(self, key, value):
        try:
            if key == "type":
                value = LoadType.value_of(value)
            elif key == "target_type":
                value = TargetType.value_of(value)
            elif key == "target_sub_type":
                value = TargetSubType.value_of(value)

            self.__setattr__(key, value)
        except Exception as ex:
            LOGGER.debug("[setattr] key=%s value=%s msg=%s", key, value, ex)

    def __str__(self):
        return json.dumps(self.__dict__)
