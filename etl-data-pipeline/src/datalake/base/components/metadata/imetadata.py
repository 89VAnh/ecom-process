from datalake.base.meta.meta_task import MetaTask
from datalake.base.utils.logger import Logger
from datalake.core.session.job import JobSession


LOGGER = Logger.get_logger("IMetadata")


class IMetadata:
    job_name: str
    scenarios: list[MetaTask]
    session: JobSession
    responses: list
    histories: list

    def __init__(self, session: JobSession):
        self.session = session
        self.job_name = session.name
        self.scenarios = []
        self.responses = []
        self.histories = []

    def load_scenarios(self):
        """
        Load scenarios from the job session.
        """
        raise NotImplementedError("load_scenarios method not implemented.")

    def save_history(self):
        """
        Save the history of the job session.
        """
        raise NotImplementedError("save_history method not implemented.")

    def get_latest_history(self):
        """
        Get the latest history of the job session.
        """
        raise NotImplementedError("get_latest_history method not implemented.")

    @staticmethod
    def init(session: JobSession):
        """
        Initialize the IMetadata instance with the given session.
        """
        from datalake.base.enums.spark import SparkEnv

        match SparkEnv.get_env(env=session.env):
            case SparkEnv.LOCAL:
                from datalake.base.components.metadata.local import LocalMetadata

                meta = LocalMetadata(session)
            case SparkEnv.GLUE:
                # from datalake.base.components.metadata.s3 import S3Metadata

                # meta = S3Metadata(session)
                from datalake.base.components.metadata.local import LocalMetadata

                meta = LocalMetadata(session)
            case _:
                LOGGER.error("Metadata Type not support = %s", type)
                raise Exception("Metadata Type not support = %s", type)
        meta.load_scenarios()
        return meta
