from enum import Enum


class IndexerStatus(str, Enum):
    """
    Indexer running status
    """

    CREATED = "Created"
    RUNNING = "Running"
    COMPLETED = "Completed"
    FAILED = "Failed"
    STOPPED = "Stopped"
