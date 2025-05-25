from datalake.base.components.metadata.imetadata import IMetadata
from datalake.base.enums.usecase import USType
from datalake.base.meta.meta_history import HistoryMeta
from datalake.base.utils.logger import Logger
from datalake.core.session.job import JobSession


LOGGER = Logger.get_logger("Usecase")


class Usecase:
    metadata: IMetadata

    def __init__(self, metadata: IMetadata):
        self.metadata = metadata
        self.responses = HistoryMeta()

    def process(self):
        pass

    @staticmethod
    def init(session: JobSession):
        LOGGER.info(f"[init] Use Case Type = {session.type}, env = {session.env.name}")
        metadata = IMetadata.init(session=session)

        match session.type:
            case USType.INGESTION:
                LOGGER.info("[init] Initalization IngestionUseCaseImpl")
                from datalake.core.usecases.implement.us_ingest import (
                    IngestUseCaseImplementation,
                )

                return IngestUseCaseImplementation(metadata=metadata)
            case USType.WAREHOUSING:
                LOGGER.info("[init] Initalization IngestionUseCaseImpl")
            case USType.LANDING:
                LOGGER.info("[init] Initalization IngestionUseCaseImpl")
            case USType.ENCRYPTION:
                LOGGER.info("[init] Initalization IngestionUseCaseImpl")
            case USType.DECRYPTION:
                LOGGER.info("[init] Initalization IngestionUseCaseImpl")
            case USType.SOURCING:
                LOGGER.info("[init] Initalization IngestionUseCaseImpl")
            case _:
                raise Exception(f"[process] Dont support US type = {session.type}")
