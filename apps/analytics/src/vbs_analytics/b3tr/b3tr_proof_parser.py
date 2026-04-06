import json
from decimal import Decimal
from typing import Dict

from loguru import logger

from vbd_indexer.b3tr.b3tr_impact_names import B3TR_IMPACT_NAMES


def parse_reward_proof(raw_proof: str) -> Dict[str, Decimal]:
    """
    Parses a sustainability proof
    Returns a dict of impact name and value
    """
    try:
        impacts: Dict[str, Decimal] = {}
        proof_json = json.loads(raw_proof)
        if "impact" not in proof_json:
            return {name: Decimal(0) for name in B3TR_IMPACT_NAMES}
        for field_name in B3TR_IMPACT_NAMES:
            if field_name in proof_json["impact"]:
                impact_value = Decimal(proof_json["impact"][field_name])
                impacts[field_name] = impact_value
            else:
                impacts[field_name] = Decimal(0)
        return impacts
    except Exception as e:
        if raw_proof is not None and len(raw_proof) > 0:
            logger.warning(f"Unable to parse reward proof: {raw_proof}")
        return {name: Decimal(0) for name in B3TR_IMPACT_NAMES}
