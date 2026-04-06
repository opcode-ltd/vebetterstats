from eth_utils.crypto import keccak

from vbd_indexer.b3tr.b3tr_contracts import B3TR_CONTRACTS
from vbd_indexer.b3tr.b3tr_event_decoders import decode_reward_event
from vbd_indexer.b3tr.b3tr_event_transformers import transform_reward_event
from vbd_indexer.b3tr.b3tr_models import B3TRRewardDecodedEvent, B3TRRewardEvent
from vbd_indexer.indexer.contract_event import ContractEvent

# -------------------------
# Contract event definitions
# --------------------------


# defines the b3tr reward distributed event
B3TR_REWARD_DEFINITION: ContractEvent = ContractEvent[
    B3TRRewardDecodedEvent, B3TRRewardEvent
](
    event_name="RewardDistributed",
    contract_address=B3TR_CONTRACTS["X2EarnRewardsPool"],
    solidity_signature="RewardDistributed(uint256,bytes32,address,string,address)",
    topic0=keccak(
        text="RewardDistributed(uint256,bytes32,address,string,address)"
    ).hex(),
    event_decoder=decode_reward_event,
    event_transformer=transform_reward_event,
)
