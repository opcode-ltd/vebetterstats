from dataclasses import dataclass


@dataclass(frozen=True)
class ThorClientOptions:
    """
    Options for creating a ThorClient
    """

    thor_url: str
    http_request_timeout: int = 10
