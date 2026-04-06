from dataclasses import dataclass


@dataclass(frozen=True)
class TransformedEvent:
    """
    Base type for a transformed event
    """

    block_number: int
    timestamp: int
