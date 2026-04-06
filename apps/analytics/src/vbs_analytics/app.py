import fire
from loguru import logger

from vbd_indexer.analysis.reward_analyser import get_rewards_summary
from vbd_indexer.b3tr.b3tr_apps import warm_app_name_cache
from vbd_indexer.b3tr.b3tr_events_defs import B3TR_REWARD_DEFINITION
from vbd_indexer.b3tr.b3tr_models import B3TRRewardDecodedEvent, B3TRRewardEvent
from vbd_indexer.config.app_config import THOR_ENDPOINTS
from vbd_indexer.indexer.event_indexer import EventIndexer
from vbd_indexer.indexer.indexer_options import IndexerOptions

# -----------------------------
# Logo printer
# -----------------------------


def print_logo() -> None:
    print("""
        ▗▖  ▗▖▗▄▄▖ ▗▄▄▄     ▗▄▄▄▖▗▖  ▗▖▗▄▄▄ ▗▄▄▄▖▗▖  ▗▖▗▄▄▄▖▗▄▄▖
        ▐▌  ▐▌▐▌ ▐▌▐▌  █      █  ▐▛▚▖▐▌▐▌  █▐▌    ▝▚▞▘ ▐▌   ▐▌ ▐▌
        ▐▌  ▐▌▐▛▀▚▖▐▌  █      █  ▐▌ ▝▜▌▐▌  █▐▛▀▀▘  ▐▌  ▐▛▀▀▘▐▛▀▚▖
         ▝▚▞▘ ▐▙▄▞▘▐▙▄▄▀    ▗▄█▄▖▐▌  ▐▌▐▙▄▄▀▐▙▄▄▖▗▞▘▝▚▖▐▙▄▄▖▐▌ ▐▌


        """)


# -----------------------------
# EXTRACT ROUND DATA
# -----------------------------


def _extract_rewards(round_id: int) -> None:
    """
    Extracts sustainability action rewards data
    """
    logger.info(f"Extracting rewards actions data for round: {round_id}")
    # create indexer options
    b3tr_reward_def = B3TR_REWARD_DEFINITION
    options = IndexerOptions[B3TRRewardDecodedEvent, B3TRRewardEvent](
        round_number=round_id,
        contract_address=b3tr_reward_def.contract_address,
        topic0=b3tr_reward_def.topic0,
        thor_endpoints=THOR_ENDPOINTS,
        task_block_size=240,
        delay_between_thor_requests=0.2,
        max_events_per_thor_request=1000,
        event_decoder=b3tr_reward_def.event_decoder,
        event_transformer=b3tr_reward_def.event_transformer,
    )
    # pre-warm the app name cache
    warm_app_name_cache(round_id)
    # create event indexer
    idx = EventIndexer(options)
    idx.start()
    final_status = idx.wait()  # blocks until done (or failed/stopped)
    logger.info(f"Final status: {final_status}")
    completed, total = idx.progress()
    logger.info(f"Progress: {completed}/{total}")
    logger.info(f"Results count: {len(idx.results())}")
    if not idx.error:
        idx.write_to_csv_file(f"rewards-events-round-{round_id}.csv")
    else:
        logger.warning("Indexer encountered error, no csv file will be written")


def extract(round_id: int) -> None:
    """
    Entry point for extract CLI command
    """
    if round_id < 1:
        logger.error("round_id has to be >= 1")
        raise ValueError("round_id has to be >= 1")
    _extract_rewards(round_id)


# -----------------------------
# ROUND SUMMARY
# -----------------------------


def _summarize_rewards(round_id: int) -> None:
    logger.info(f"Summarizing rewards data for round: {round_id}")
    # do the analysis
    df_summary = get_rewards_summary(round_id)
    file_name = f"reward-events-summary-round-{round_id}.json"
    df_summary.to_json(file_name, orient="records", indent=2)
    logger.info(f"Analysis saved to file: {file_name}")


def summarize(round_id: int) -> None:
    """
    Analyses extracted round data CSV file
    Produces a json file of statistics
    """
    if round_id < 1:
        logger.error("round_id has to be >= 1")
        raise ValueError("round_id has to be >= 1")
    _summarize_rewards(round_id)


# -----------------------------
# Entry point
# -----------------------------


def main() -> None:
    print_logo()
    fire.Fire({"extract": extract, "summarize": summarize})


if __name__ == "__main__":
    main()
