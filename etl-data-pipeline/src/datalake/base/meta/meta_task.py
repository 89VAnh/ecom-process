from datalake.base.meta.meta_job import ExtractMeta, LoadMeta
from datalake.base.utils.logger import Logger

LOGGER = Logger.get_logger(__name__)


class MetaTask:
    extracts: list[ExtractMeta]
    transform: str
    load: LoadMeta
    job_name: str

    def __init__(self, scenario: dict = None):
        self.job_name = scenario.get("job_name")
        self.extracts = []
        self.udf_infos = []
        self.transform = None
        self.load = None
        if scenario is None:
            scenario = {}
        if "extracts" in scenario.keys():
            extracts_data = scenario.get("extracts")
            for extract in extracts_data:
                self.extracts.append(ExtractMeta(extract))
        if "udf_infos" in scenario.keys():
            self.udf_infos = scenario.get("udf_infos")
        if "transform" in scenario.keys():
            self.transform = scenario.get("transform")
        if "load" in scenario.keys():
            self.load = LoadMeta(scenario.get("load"))
