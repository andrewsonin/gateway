from abc import ABCMeta, abstractmethod

import pandas as pd
import pandera as pa

__all__ = ('Node',)


class Node(metaclass=ABCMeta):
    __slots__ = ('__gateway_id', '__already_cached', '__output_validator')
    __instance_counter = 0

    def __init__(self,
                 *,
                 already_cached: bool,
                 output_validator: pa.DataFrameSchema) -> None:
        self.__gateway_id = Node.__instance_counter
        Node.__instance_counter += 1
        self.__already_cached = already_cached
        self.__output_validator = output_validator

    @property
    def already_cached(self) -> bool:
        return self.__already_cached

    @property
    @abstractmethod
    def use_cached(self) -> bool:
        pass

    def extract_data(self) -> pd.DataFrame:
        if self.already_cached:
            return self._load_cached()
        data = self._load_non_cached()
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
    def _load_non_cached(self) -> pd.DataFrame:
        pass
