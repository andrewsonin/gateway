from abc import ABCMeta, abstractmethod

import pandas as pd

__all__ = ('Node',)


class Node(metaclass=ABCMeta):
    __slots__ = ('_object_id',)
    __instance_counter = 0

    def __init__(self):
        self._object_id = Node.__instance_counter
        Node.__instance_counter += 1

    @abstractmethod
    def get(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def hang_on(self, *positional_nodes: 'Node', **keyword_nodes: 'Node') -> None:
        pass

    @property
    @abstractmethod
    def use_cached(self) -> bool:
        pass

    def dump_to_cache(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("dump_to_cache is not implemented")

    def load_cached(self) -> pd.DataFrame:
        raise NotImplementedError("load_cached is not implemented")
