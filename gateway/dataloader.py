from abc import abstractmethod

import pandas as pd
import pandera as pa

from gateway.node import Node
from gateway.validators import AnyDataFrame

__all__ = (
    'DataLoader',
)


class DataLoader(Node):
    __slots__ = ('already_cached', '_output_validator')

    def __init__(self, output_validator: pa.DataFrameSchema = AnyDataFrame) -> None:
        super().__init__()
        self.already_cached: bool = False
        self._output_validator = output_validator

    def get(self) -> pd.DataFrame:
        if self.use_cached:
            if self.already_cached:
                return self.load_cached()
            data = self._output_validator.validate(self.load())
            self.dump_to_cache(data)
            self.already_cached = True
            return data
        return self._output_validator.validate(self.load())

    def hang_on(self, *positional_nodes: Node, **keyword_nodes: Node) -> None:
        raise NotImplementedError("DataLoader instance cannot be hung on any other Node instances")

    @abstractmethod
    def load(self) -> pd.DataFrame:
        pass
