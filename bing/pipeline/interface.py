from typing import Callable, Any
from dataclasses import dataclass
from datetime import date

from bingads.service_client import ServiceClient


@dataclass
class Pipeline:
    name: str
    build_fn: Callable[[ServiceClient], Callable[[str, tuple[date, date]], Any]]
    transform_fn: Callable[[Any], list[dict]]
    schema: list[dict]
