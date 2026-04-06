from dataclasses import dataclass


@dataclass(frozen=True)
class DecodedEvent:
    """
    Base type for a decoded event
    """

    block_number: int
    timestamp: int
