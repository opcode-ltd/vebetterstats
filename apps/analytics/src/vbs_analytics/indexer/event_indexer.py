import queue
import threading
from dataclasses import asdict
from typing import Generic, List, Optional, Tuple, TypeVar

import pandas as pd
from loguru import logger

from vbd_indexer.b3tr.b3tr_round import get_block_range_for_round
from vbd_indexer.thor.thor_client import ThorClient
from vbd_indexer.thor.thor_client_options import ThorClientOptions

from .decoded_event import DecodedEvent
from .indexer_options import IndexerOptions
from .indexer_status import IndexerStatus
from .indexer_task import IndexerTask
from .transformed_event import TransformedEvent

# -----------------------------
# Indexer
# -----------------------------

EDecoded = TypeVar("EDecoded", bound=DecodedEvent)
ETransformed = TypeVar("ETransformed", bound=TransformedEvent)


class EventIndexer(Generic[EDecoded, ETransformed]):
    """
    Spawns one worker per thor endpoint. Each worker:
      - pulls an IndexerTask from a shared queue
      - appends results into a shared structure (thread-safe)
    The main thread can call wait() until status is COMPLETED/FAILED/STOPPED.
    """

    def __init__(self, options: IndexerOptions[EDecoded, ETransformed]) -> None:
        if options.round_number <= 0:
            raise ValueError("round_id must be >= 1")
        if not options.thor_endpoints:
            raise ValueError("endpoints must not be empty")
        if options.task_block_size <= 0:
            raise ValueError("task_block_size must be > 0")
        if options.max_events_per_thor_request > 1000:
            raise ValueError("max_events_per_thor_request must be <= 1000")
        if options.max_events_per_thor_request < 1:
            raise ValueError("max_events_per_thor_request must be > 0")
        if options.delay_between_thor_requests <= 0:
            raise ValueError("delay_between_thor_requests must be > 0")

        # save options
        self.options = options

        self._tasks: "queue.Queue[IndexerTask]" = queue.Queue()
        self._threads: List[threading.Thread] = []
        self._stop_event = threading.Event()

        self._status_lock = threading.Lock()
        self._status: IndexerStatus = IndexerStatus.CREATED
        self._error: Optional[BaseException] = None

        # Shared results structure
        self._results_lock = threading.Lock()
        self._results: List[ETransformed] = []

        # For tracking progress
        self._total_tasks = 0
        self._completed_tasks = 0
        self._progress_lock = threading.Lock()

    # --------
    # Public API
    # --------

    @property
    def status(self) -> IndexerStatus:
        with self._status_lock:
            return self._status

    @property
    def error(self) -> Optional[BaseException]:
        with self._status_lock:
            return self._error

    def results(self) -> List[ETransformed]:
        # Return a snapshot copy
        with self._results_lock:
            return list(self._results)

    def progress(self) -> Tuple[int, int]:
        """(completed_tasks, total_tasks)"""
        with self._progress_lock:
            return self._completed_tasks, self._total_tasks

    def start(self) -> None:
        if self.status not in (IndexerStatus.CREATED, IndexerStatus.STOPPED):
            raise RuntimeError(f"Cannot start Indexer in state {self.status}")
        logger.info("Starting indexing")
        # get block start and end for round number
        self.block_start, self.block_end = get_block_range_for_round(
            self.options.round_number
        )
        self._build_task_queue()
        with self._status_lock:
            self._status = IndexerStatus.RUNNING
            self._error = None
        self._stop_event.clear()
        self._threads = []

        for i, endpoint in enumerate(self.options.thor_endpoints):
            t = threading.Thread(
                target=self._worker_loop,
                name=f"indexer-worker-{i}",
                args=(endpoint,),
                daemon=True,
            )
            self._threads.append(t)
            t.start()

        # Optionally: a monitor thread could be used, but wait() can do it too.

    def stop(self) -> None:
        """Request workers stop ASAP (cooperative)."""
        self._stop_event.set()
        with self._status_lock:
            if self._status == IndexerStatus.RUNNING:
                self._status = IndexerStatus.STOPPED

    def wait(self, timeout: Optional[float] = None) -> IndexerStatus:
        """
        Block until all work is done, failed, or stopped.
        Returns final status.
        """
        # Join worker threads
        for t in self._threads:
            t.join(timeout=timeout)

        # If still running after timeout, return current status
        # (threads may not be finished)
        if any(t.is_alive() for t in self._threads):
            return self.status

        # All threads finished: if still RUNNING, we completed successfully
        with self._status_lock:
            if self._status == IndexerStatus.RUNNING:
                self._status = IndexerStatus.COMPLETED
            return self._status

    def write_to_csv_file(self, filename: str) -> None:
        """
        Writes indexed events to CSV file
        """
        if self.status in (IndexerStatus.CREATED, IndexerStatus.RUNNING):
            raise RuntimeError(
                f"Cannot save events to file when indexer in state: {self.status}"
            )
        logger.info(f"Writing csv file {filename}")
        with self._results_lock:
            records = [asdict(e) for e in self._results]
            df = pd.json_normalize(
                records, sep="_"
            )  # want to flattern any nested Dicts to new columns
            df.to_csv(filename, index=False)

    def clear_results(self) -> None:
        """
        Clear the indexing results
        """
        with self._status_lock:
            if self._status == IndexerStatus.RUNNING:
                raise RuntimeError("Cannot clear results while indexing is in progress")
        with self._results_lock:
            self._results.clear()

    # --------
    # Internals
    # --------

    def _build_task_queue(self) -> None:
        # Clear any previous queue contents by replacing the queue
        self._tasks = queue.Queue()

        start = self.block_start
        end = self.block_end
        step = self.options.task_block_size

        tasks = 0
        b = start
        while b <= end:
            chunk_end = min(b + step - 1, end)
            self._tasks.put(IndexerTask(start_block=b, end_block=chunk_end))
            tasks += 1
            b = chunk_end + 1

        with self._progress_lock:
            self._total_tasks = tasks
            self._completed_tasks = 0

        logger.info(f"Created {self._total_tasks} indexing tasks")

        # Add sentinels (one per worker) for clean shutdown
        for _ in self.options.thor_endpoints:
            self._tasks.put(IndexerTask(-1, -1))  # sentinel

    # ------------
    # Worker Loop
    # ------------

    def _worker_loop(self, endpoint: str) -> None:
        # create a thor client
        thor_client_options = ThorClientOptions(thor_url=endpoint)
        thor_client = ThorClient(thor_client_options)
        try:
            # loop until cancelled or sentinal task reached
            while not self._stop_event.is_set():
                task = self._tasks.get()

                try:
                    # Sentinel means "no more work"
                    if task.start_block == -1 and task.end_block == -1:
                        return

                    # Get raw events from thor with decoder
                    raw_events = thor_client.get_events(
                        from_block=task.start_block,
                        to_block=task.end_block,
                        contract_address=self.options.contract_address,
                        topic0=self.options.topic0,
                        max_events_per_request=self.options.max_events_per_thor_request,
                        delay_between_requests=self.options.delay_between_thor_requests,
                    )

                    # decode the raw events
                    decoded_events = [
                        self.options.event_decoder(raw_event)
                        for raw_event in raw_events
                    ]

                    # transform decoded events to final type
                    trans_events = [
                        self.options.event_transformer(decoded_event)
                        for decoded_event in decoded_events
                    ]
                    # remove any None values - where transformer couldnt transform
                    cleaned_trans_events = [e for e in trans_events if e is not None]
                    raw_events = []
                    decoded_events = []
                    trans_events = []

                    # Contribute results (shared structure)
                    if cleaned_trans_events:
                        with self._results_lock:
                            self._results.extend(cleaned_trans_events)

                    # Progress tracking
                    with self._progress_lock:
                        self._completed_tasks += 1

                finally:
                    self._tasks.task_done()

        except BaseException as e:
            # Mark failed and stop all workers
            with self._status_lock:
                self._status = IndexerStatus.FAILED
                self._error = e
            self._stop_event.set()
        finally:
            thor_client.dispose()
