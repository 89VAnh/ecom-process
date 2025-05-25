import json

from datalake.base.components.metadata.imetadata import IMetadata
from datalake.base.meta.meta_task import MetaTask
from datalake.base.meta.meta_history import HistoryMeta
from datalake.base.utils.date import DateUtils
from datalake.base.utils.logger import Logger

LOGGER = Logger.get_logger(__name__)


class LocalMetadata(IMetadata):

    def __init__(self, session):
        super().__init__(session)
        self.history_path = "/home/glue_user/etl-data-pipeline/histories/history.json"

    def load_scenarios(self):
        with open(
            f"/home/glue_user/etl-data-pipeline/scenarioes/{self.job_name}.json",
            mode="r",
        ) as file:
            data = json.load(file)
        metas = [x for x in data if x["status"] == True]
        for meta in metas:
            self.scenarios.append(MetaTask(meta))

    def save_history(self, history):
        LOGGER.debug("history response from scenario: %s", history.__dict__)
        with open(self.history_path, mode="r") as f:
            histories = json.load(f)
        if history.criteria_value:
            history.criteria_value = DateUtils.strftime(
                history.criteria_value, "%Y-%m-%d %H:%M:%S"
            )
        histories.append(history.__dict__)
        with open(self.history_path, mode="w") as f:
            f.write(json.dumps(histories))

    def get_latest_history(self, meta: MetaTask):
        with open(self.history_path, mode="r") as f:
            histories: list[HistoryMeta] = [HistoryMeta(hist) for hist in json.load(f)]
        latest_date = DateUtils.min_date()
        for history in histories:
            if history.job_name == meta.job_name and history.status == "SUCCESS":
                if history.criteria_value:
                    criteria_val = DateUtils.strptime(
                        history.criteria_value, "%Y-%m-%d %H:%M:%S"
                    )
                    if criteria_val > latest_date:
                        latest_date = criteria_val
        return latest_date
