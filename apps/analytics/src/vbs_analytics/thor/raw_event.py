from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class RawEvent:
    """
    Raw (un-decoded) event details
    """

    block_number: int
    timestamp: int
    data: str
    topics: List[str]
