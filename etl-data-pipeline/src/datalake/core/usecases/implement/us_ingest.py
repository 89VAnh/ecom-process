import json

from datalake.base.meta.meta_task import MetaTask
from datalake.base.utils.date import DateUtils
from datalake.base.utils.logger import Logger
from datalake.core.session.spark import SparkSession
from datalake.core.factory.etl import ETLFactory
from datalake.core.usecases import Usecase

LOGGER = Logger.get_logger("IngestUseCaseImplementation")


class IngestUseCaseImplementation(Usecase):
    def process(self):
        LOGGER.info(
            f"[process] starting ingestion with job_name = {self.metadata.job_name}"
        )
        spark_session = SparkSession(metadata=self.metadata)
        self.responses.job_name = self.metadata.job_name
        errors = []
        for scenario in self.metadata.scenarios:
            try:
                params = self.get_params(scenario)
                self.responses.source_object = scenario.extracts[0].source_object
                LOGGER.debug(
                    "[process] scenario = %s",
                    json.dumps(scenario.extracts[0].source_object),
                )
                LOGGER.info("[process] Execute Pre Function...")
                LOGGER.info("[process] Extracting...")
                extract_result = ETLFactory.extract(
                    spark=spark_session.spark,
                    extract_meta=scenario.extracts[0],
                    args=params,
                )
                df = extract_result.get("data")
                if df is None:
                    LOGGER.warning("[process] : extract data is empty")
                    return
                LOGGER.info("[process] Transforming...")
                df = ETLFactory.transforms(
                    spark=spark_session.spark,
                    dataframe=df,
                    transform_func=scenario.transform,
                )
                LOGGER.info("[process] Loading...")
                ETLFactory.load(
                    spark=spark_session.spark, dataframe=df, load_meta=scenario.load
                )
                LOGGER.info("[process] Execute Post Function...")
                self.responses.status = "SUCCESS"
                self.responses.criteria_value = extract_result.get("criteria_value")
            except Exception as e:
                self.responses.status = "FAILED"
                self.responses.message = str(e)
                errors.append(str(e))
            self.metadata.save_history(self.responses)
            if len(errors) > 0:
                raise Exception(str(errors))

    def get_params(self, meta: MetaTask):
        latest_date = self.metadata.get_latest_history(meta)
        LOGGER.debug(f"latest date from history: {latest_date}")
        to_date = DateUtils.to_utc(DateUtils.now())
        LOGGER.debug(f"to_date: {to_date}")
        return {"from_date": latest_date, "to_date": to_date}
