import json
import threading
from typing import Any
from abc import ABC, abstractmethod


class AbstractKVDB(ABC):

    @abstractmethod
    def get(self, key: str) -> Any:
        pass

    @abstractmethod
    def set(self, key: str, value: Any):
        pass

    @abstractmethod
    def delete(self, key: str):
        pass
