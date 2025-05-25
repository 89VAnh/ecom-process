import json

from datalake.base.components.metadata.imetadata import IMetadata
from datalake.base.meta.meta_task import MetaTask
from datalake.base.meta.meta_history import HistoryMeta
from datalake.base.utils.date import DateUtils
from datalake.base.utils.data import DataUtils
from datalake.base.utils.logger import Logger

LOGGER = Logger.get_logger(__name__)


class S3Metadata(IMetadata):
    metadata_bucket = "vietanh21-ecom-crawl-data"
    history_object_key = "metadata/history/history.json"

    def load_scenarios(self):
        scenario_object_key = f"metadata/scenarios/{self.job_name}.json"
        with DataUtils.read_s3_object(self.metadata_bucket, scenario_object_key) as f:
            data = json.load(f)
        metas = [x for x in data if x["status"] == True]
        for meta in metas:
            self.scenarios.append(MetaTask(meta))

    def save_history(self, history):
        LOGGER.debug("history response from scenario: %s", history.__dict__)
        with DataUtils.read_s3_object(
            self.metadata_bucket, self.history_object_key
        ) as f:
            histories = json.load(f)
        if history.criteria_value:
            history.criteria_value = DateUtils.strftime(
                history.criteria_value, "%Y-%m-%d %H:%M:%S"
            )
        histories.append(history.__dict__)
        DataUtils.write_s3_object(
            self.metadata_bucket,
            self.history_object_key,
            json.dumps(histories).encode("utf-8"),
        )

    def get_latest_history(self, meta: MetaTask):
        with DataUtils.read_s3_object(
            self.metadata_bucket, self.history_object_key
        ) as f:
            data = json.load(f)
        histories: list[HistoryMeta] = [HistoryMeta(hist) for hist in data]
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
