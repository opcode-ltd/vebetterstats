from eth_abi.abi import decode
from eth_utils.address import to_checksum_address

from vbd_indexer.b3tr.b3tr_models import B3TRRewardDecodedEvent
from vbd_indexer.thor.raw_event import RawEvent


def decode_reward_event(raw_event: RawEvent) -> B3TRRewardDecodedEvent:
    """
    Decodes an event to a B3TRRewardEvent class
    This event has signature:
    event RewardDistributed(
      uint256 amount,
      bytes32 indexed appId,
      address indexed receiver,
      string proof,
      address indexed distributor
    );
    """
    # indexed data
    app_id = raw_event.topics[1]
    receiver_address = to_checksum_address("0x" + raw_event.topics[2][-40:])
    distributor_address = to_checksum_address("0x" + raw_event.topics[3][-40:])
    # --- non-indexed fields from data ---
    # order: uint256 amount, string proof
    reward_amount, reward_proof = decode(
        ["uint256", "string"],
        bytes.fromhex(raw_event.data[2:]),
    )
    # return event
    return B3TRRewardDecodedEvent(
        block_number=raw_event.block_number,
        timestamp=raw_event.timestamp,
        amount=reward_amount,
        receiver_address=receiver_address,
        proof=reward_proof,
        app_id=app_id,
        distributor_address=distributor_address,
    )
