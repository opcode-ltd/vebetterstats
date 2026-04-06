from loguru import logger

from vbd_indexer.b3tr.b3tr_apps import get_app_name
from vbd_indexer.b3tr.b3tr_models import B3TRRewardDecodedEvent, B3TRRewardEvent
from vbd_indexer.b3tr.b3tr_proof_parser import parse_reward_proof
from vbd_indexer.utils.units import format_wei


def transform_reward_event(raw_event: B3TRRewardDecodedEvent) -> B3TRRewardEvent | None:
    """
    Transform a raw Reward event into a final Reward event
    - amount (wei) to b3tr
    - app name is filled
    - impacts are extracted from proof json
    """
    try:
        # get transformed fields
        b3tr_amount = format_wei(raw_event.amount)
        app_name = get_app_name(raw_event.app_id)
        if app_name is None:
            # blacklisted app
            return None
        proof_impacts = parse_reward_proof(raw_event.proof)

        # return reward event
        return B3TRRewardEvent(
            block_number=raw_event.block_number,
            timestamp=raw_event.timestamp,
            amount=b3tr_amount,
            app_id=raw_event.app_id,
            app_name=app_name,
            receiver_address=raw_event.receiver_address,
            impact=proof_impacts,
        )
    except Exception as e:
        logger.error(f"Error transforming RewardRawEvent: {e}")
        raise
