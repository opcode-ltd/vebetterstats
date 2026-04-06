from dataclasses import dataclass
from decimal import Decimal
from typing import Dict

from vbd_indexer.indexer.decoded_event import DecodedEvent
from vbd_indexer.indexer.transformed_event import TransformedEvent

# ---------------------------
# Indexed Event objects
# ---------------------------


@dataclass(frozen=True)
class B3TRRewardDecodedEvent(DecodedEvent):
    """
    Data gathered from the direct decoding of a "RewardDistributed" solidity event
    """

    amount: int
    app_id: str
    receiver_address: str
    proof: str
    distributor_address: str


# -----------------------------
# Transformed Event objects
# -----------------------------


@dataclass(frozen=True)
class B3TRRewardEvent(TransformedEvent):
    """
    A transformed/sanitised B3TRRewardRawEvent
    """

    amount: Decimal
    app_id: str
    app_name: str
    receiver_address: str
    impact: Dict[str, Decimal]
