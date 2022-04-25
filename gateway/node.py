from abc import ABCMeta, abstractmethod
from collections import defaultdict
from typing import Dict, Set

import pandas as pd
import pandera as pa

__all__ = ('Node',)


class Node(metaclass=ABCMeta):
    __slots__ = ('__gateway_id', '__already_cached', '__output_validator')
    __instance_counter = 0
    __parental_graph: Dict['Node', Set['Node']] = defaultdict(set)
    __children_graph: Dict['Node', Set['Node']] = defaultdict(set)

    def __init__(self,
                 *,
                 already_cached: bool,
                 output_validator: pa.DataFrameSchema) -> None:
        self.__gateway_id = Node.__instance_counter
        Node.__instance_counter += 1
        self.__already_cached = already_cached
        self.__output_validator = output_validator

    def __hash__(self) -> int:
        return self.__gateway_id

    def _add_edge_to_connection_graph(self, parent_node: 'Node') -> None:
        Node.__parental_graph[self].add(parent_node)
        Node.__children_graph[parent_node].add(self)

    def reset_cache_status(self, *, _reset_in_children: bool = True) -> None:
        self.__already_cached = False
        if _reset_in_children:
            children_graph = Node.__children_graph
            children = children_graph[self].copy()
            # for child in
            for child in Node.__children_graph[self]:
                child.reset_cache_status(_reset_in_children=False)

    @property
    def already_cached(self) -> bool:
        return self.__already_cached

    @property
    @abstractmethod
    def use_cached(self) -> bool:
        pass

    def extract_data(self, *, _assert_no_cycles: bool = True) -> pd.DataFrame:
        if self.already_cached:
            return self._load_cached()
        data = self._load_non_cached(_assert_no_cycles=_assert_no_cycles)
        data = self.transform_data(data)
        data = self._output_validator.validate(data)
        if self.use_cached:
            self._dump_to_cache(data)
            self.__already_cached = True
        return data

    @abstractmethod
    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        pass

    @property
    def _gateway_id(self) -> int:
        return self.__gateway_id

    @property
    def _output_validator(self) -> pa.DataFrameSchema:
        return self.__output_validator

    @abstractmethod
    def _dump_to_cache(self, data: pd.DataFrame) -> None:
        pass

    @abstractmethod
    def _load_cached(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def _load_non_cached(self, *, _assert_no_cycles: bool = True) -> pd.DataFrame:
        pass
