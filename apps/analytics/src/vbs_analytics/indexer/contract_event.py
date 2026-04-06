from dataclasses import dataclass
from typing import Callable, Generic, TypeVar

from vbd_indexer.thor.raw_event import RawEvent

from .decoded_event import DecodedEvent
from .transformed_event import TransformedEvent

EDecoded = TypeVar("EDecoded", bound=DecodedEvent)
ETransformed = TypeVar("ETransformed", bound=TransformedEvent)


@dataclass(frozen=True)
class ContractEvent(Generic[EDecoded, ETransformed]):
    """
    Defines an index-able event detals
    """

    event_name: str
    contract_address: str
    solidity_signature: str
    topic0: str
    event_decoder: Callable[[RawEvent], EDecoded]
    event_transformer: Callable[[EDecoded], ETransformed | None]
