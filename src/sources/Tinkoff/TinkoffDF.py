from typing import Dict, Any

from src.sources.Tinkoff import DocsAPI


class TinkoffDF:

    def __init__(self, key=None):
        if key:
            self.key: str = key
            self.url: str = getattr(DocsAPI, key).url
            self.payload: Dict[str, Any] = getattr(DocsAPI, key)().payload()
            self.requestType: DocsAPI.RequestType = getattr(DocsAPI, key).requestType
