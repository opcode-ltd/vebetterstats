from dataclasses import dataclass
from typing import Callable, Generic, List, TypeVar

from vbd_indexer.indexer.decoded_event import DecodedEvent
from vbd_indexer.indexer.transformed_event import TransformedEvent
from vbd_indexer.thor.raw_event import RawEvent

EDecoded = TypeVar("EDecoded", bound=DecodedEvent)
ETransformed = TypeVar("ETransformed", bound=TransformedEvent)


@dataclass(frozen=True)
class IndexerOptions(Generic[EDecoded, ETransformed]):
    """
    Indexer options
    """

    round_number: int
    contract_address: str
    topic0: str
    thor_endpoints: List[str]
    task_block_size: int
    max_events_per_thor_request: int
    delay_between_thor_requests: float
    event_decoder: Callable[[RawEvent], EDecoded]
    event_transformer: Callable[[EDecoded], ETransformed | None]
