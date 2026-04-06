from typing import Tuple

from eth_abi.abi import decode, encode
from eth_utils.crypto import keccak
from loguru import logger

from vbd_indexer.b3tr.b3tr_contracts import B3TR_CONTRACTS
from vbd_indexer.config.app_config import DEFAULT_THOR_ENDPOINT
from vbd_indexer.thor.thor_client import ThorClient
from vbd_indexer.thor.thor_client_options import ThorClientOptions


def get_block_range_for_round(round_number: int) -> Tuple[int, int]:
    """
    Gets the start and end block number for a VBD round
    """
    client_options = ThorClientOptions(
        thor_url=DEFAULT_THOR_ENDPOINT, http_request_timeout=10
    )
    thor_client = ThorClient(client_options)
    try:
        # encode the function call data
        solidity_sig = "getRound(uint256)"
        func_selector = keccak(text=solidity_sig)[:4]
        encoded_args = encode(["uint256"], [round_number])
        encoded_call_data = "0x" + (func_selector + encoded_args).hex()
        # get the raw response
        response_data = thor_client.call_contract(
            contract_address=B3TR_CONTRACTS["XAllocationVoting"],
            call_data=encoded_call_data,
        )
        # decode the response
        raw = bytes.fromhex(response_data[2:])  # strip 0x
        proposer, vote_start, vote_duration = decode(
            ["address", "uint48", "uint32"],
            raw,
        )
        logger.info(
            f"Round: {round_number} start_block: {vote_start} block_length: {vote_duration}"
        )
        return vote_start, (vote_start + vote_duration)
    finally:
        thor_client.dispose()
