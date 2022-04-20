from abc import abstractmethod
from io import RawIOBase, BufferedIOBase, TextIOBase, TextIOWrapper
from mmap import mmap
from os import PathLike
from typing import Tuple, Any, Union, IO, Optional, Callable, Dict

import pandas as pd
import pandera as pa

from gateway.node import Node
from gateway.validators import AnyDataFrame

__all__ = (
    'DataLoader',
)


class DataLoader(Node):
    __slots__ = ()

    @abstractmethod
    def _load(self) -> pd.DataFrame:
        pass

    def _load_non_cached(self) -> pd.DataFrame:
        return self._load()

    def hang_on_parental_nodes(self, *positional_nodes: Node, **keyword_nodes: Node) -> None:
        raise NotImplementedError("DataLoader instance cannot be hung on any other Node instances")


class StaticDataLoader(DataLoader):
    __slots__ = ('__loader', '__loader_args', '__loader_kwargs')
    use_cached = True

    def __init__(self,
                 loader: Callable[..., pd.DataFrame],
                 *loader_args: Any,
                 output_validator: pa.DataFrameSchema = AnyDataFrame,
                 **loader_kwargs: Any) -> None:
        super().__init__(output_validator=output_validator, already_cached=True)
        self.__loader = loader
        self.__loader_args = loader_args
        self.__loader_kwargs = loader_kwargs

    def _load_cached(self) -> pd.DataFrame:
        return self.__loader(*self.__loader_args, **self.__loader_kwargs)

    def _load_non_cached(self) -> pd.DataFrame:
        raise NotImplementedError("StaticDataLoader does not implement `_load_non_cached`. Use `load_cached` instead")

    def _load(self) -> pd.DataFrame:
        raise NotImplementedError("StaticDataLoader does not implement `load`. Use `load_cached` instead")


PD_READ_PICKLE_ANNOTATION = Union[
    PathLike,
    str,
    IO[str],
    RawIOBase,
    BufferedIOBase,
    TextIOBase,
    TextIOWrapper,
    mmap
]


class PickleLoader(StaticDataLoader):
    __slots__ = ('__filepath_or_buffer', '__compression')

    def __init__(self,
                 filepath_or_buffer: PD_READ_PICKLE_ANNOTATION,
                 compression: Optional[str] = 'infer',
                 *,
                 output_validator: pa.DataFrameSchema = AnyDataFrame) -> None:
        super().__init__(
            pd.read_pickle,
            filepath_or_buffer,
            compression,
            output_validator=output_validator
        )
        self.__filepath_or_buffer = filepath_or_buffer
        self.__compression = compression

    @property
    def compression(self) -> Optional[str]:
        return self.__compression

    @property
    def filepath_or_buffer(self) -> PD_READ_PICKLE_ANNOTATION:
        return self.__filepath_or_buffer


if __name__ == '__main__':
    print(PickleLoader('f').extract_data())
