import time
from typing import List, Optional

import httpx

from vbd_indexer.thor.raw_event import RawEvent
from vbd_indexer.thor.thor_client_options import ThorClientOptions


class ThorClient:
    def __init__(self, options: ThorClientOptions) -> None:
        self.options = options
        self._client: Optional[httpx.Client] = httpx.Client(
            base_url=self.options.thor_url, timeout=self.options.http_request_timeout
        )

    def dispose(self) -> None:
        """
        Dispose the http client
        """
        if self._client is not None:
            self._client.close()
            self._client = None

    def get_events(
        self,
        from_block: int,
        to_block: int,
        contract_address: str,
        topic0: str,
        max_events_per_request: int,
        delay_between_requests: float,
    ) -> List[RawEvent]:
        """
        Post requests to thor to get the events
        Each request is for max_events_per_request events, so pagination is used to get all events
        Between requests the current thread is paused for delay_between_requests (secs), to avoid rate limiting
        Errors are not caught here, they go back to the caller
        """
        if self._client is None:
            raise RuntimeError("ThorClient is disposed")
        offset = 0
        all_pages_received = False
        all_events: List[RawEvent] = []
        while not all_pages_received:
            # sleep between requests
            time.sleep(delay_between_requests)
            # get events for page
            page_events = self._send_get_events(
                from_block,
                to_block,
                contract_address,
                topic0,
                max_events_per_request,
                offset,
            )
            # add to all paged events
            all_events.extend(page_events)
            # check if last page
            if len(page_events) < max_events_per_request:
                all_pages_received = True
            else:
                offset = offset + max_events_per_request
        return all_events

    def _send_get_events(
        self,
        from_block: int,
        to_block: int,
        contract_address: str,
        topic0: str,
        max_events: int,
        offset: int,
    ) -> List[RawEvent]:
        """
        Makes a single request to thor to get events
        """
        if self._client is None:
            raise RuntimeError("ThorClient is disposed")
        # build post data
        post_data = {
            "range": {"unit": "block", "from": from_block, "to": to_block},
            "options": {"offset": offset, "limit": max_events, "includeIndexes": True},
            "criteriaSet": [{"address": contract_address, "topic0": topic0}],
        }
        # do http post
        response = self._client.post("/logs/event", json=post_data)
        response.raise_for_status()
        response_json = response.json()
        # process events from response
        events: List[RawEvent] = []
        for event in response_json:
            events.append(
                RawEvent(
                    block_number=event["meta"]["blockNumber"],
                    timestamp=event["meta"]["blockTimestamp"],
                    data=event["data"],
                    topics=event["topics"],
                )
            )
        return events

    def call_contract(self, contract_address: str, call_data: str) -> str:
        """
        Performs a contract call with the specified call data
        Returns the json data response without decoding
        """
        if self._client is None:
            raise RuntimeError("ThorClient is disposed")
        # build post data
        post_data = {
            "clauses": [{"to": contract_address, "value": "0", "data": call_data}]
        }
        # do the post request
        response = self._client.post("/accounts/*", json=post_data)
        response.raise_for_status()
        response_json = response.json()
        # get data from response
        response_data = response_json[0]["data"]
        return response_data
