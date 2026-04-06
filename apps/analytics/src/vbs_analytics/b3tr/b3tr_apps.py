from typing import Dict

from eth_abi.abi import decode, encode
from eth_utils.crypto import keccak
from loguru import logger

from vbd_indexer.b3tr.b3tr_contracts import B3TR_CONTRACTS
from vbd_indexer.config.app_config import DEFAULT_THOR_ENDPOINT
from vbd_indexer.thor.thor_client import ThorClient, ThorClientOptions

# cached values
_cached_app_map: Dict[str, str] | None = None
_cached_round: int | None = None


def warm_app_name_cache(round_number: int) -> None:
    """
    Gets a list of app ids and app names for the round number
    Only need to call this once as it populates above caches
    """
    global _cached_app_map, _cached_round
    if round_number != _cached_round:
        # clear cache if different round
        if _cached_app_map is not None:
            _cached_app_map.clear()
    if round_number == _cached_round and _cached_app_map is not None:
        # raise error if same round and already cached
        raise ValueError(f"App names are already cached for round: {round_number}")
    # setup cache
    _cached_round = round_number
    _cached_app_map = {}
    # call contract function
    client_options = ThorClientOptions(
        thor_url=DEFAULT_THOR_ENDPOINT, http_request_timeout=10
    )
    thor_client = ThorClient(client_options)
    try:
        logger.info(f"Getting app names for round {round_number}")
        # encode the function call data
        solidity_sig = "getAppsOfRound(uint256)"
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
        app_type = "(bytes32,address,string,string,uint256,bool)[]"
        apps = decode([app_type], raw)[0]
        # extract only id and name
        for app in apps:
            app_id = "0x" + app[0].hex().lower()
            name = app[2]
            _cached_app_map[app_id] = name
        logger.info(f"Round {round_number} has {len(_cached_app_map)} active apps")
    finally:
        thor_client.dispose()


def get_app_name(app_id: str) -> str | None:
    """
    Returns the app name for an id, using cached round data.
    If not found, returns None.
    """
    if _cached_app_map is None:
        raise RuntimeError(
            "Cache not warmed – call warm_app_name_cache(round_number) first"
        )
    app_name = _cached_app_map.get(app_id.lower())
    if app_name is None:
        logger.warning(f"No app name found for app id: {app_id}")
        app_name = None
    return app_name
