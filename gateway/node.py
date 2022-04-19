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
