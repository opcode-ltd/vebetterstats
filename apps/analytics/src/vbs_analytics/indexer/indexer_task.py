from dataclasses import dataclass


@dataclass(frozen=True)
class IndexerTask:
    """
    Class to represent a unit of indexing work
    """

    start_block: int
    end_block: int
