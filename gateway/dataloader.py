from abc import abstractmethod

import pandas as pd
import pandera as pa

from gateway.node import Node
from gateway.validators import AnyDataFrame

__all__ = (
    'DataLoader',
)


class DataLoader(Node):
    __slots__ = ('_already_cached', '_output_validator')

    def __init__(self, output_validator: pa.DataFrameSchema = AnyDataFrame) -> None:
        super().__init__()
        self._already_cached: bool = False
        self._output_validator = output_validator

    def get(self) -> pd.DataFrame:
        if self.use_cached:
            if self._already_cached:
                return self.load_cached()
            data = self._output_validator.validate(self.load())
            self.dump_to_cache(data)
            return data
        return self._output_validator.validate(self.load())

    @abstractmethod
    def load(self) -> pd.DataFrame:
        pass

    @property
    @abstractmethod
    def use_cached(self) -> bool:
        pass

    def dump_to_cache(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("dump_to_cache is not implemented")

    def load_cached(self) -> pd.DataFrame:
        raise NotImplementedError("load_cached is not implemented")
