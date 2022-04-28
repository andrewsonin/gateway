from typing import Any, Optional, Callable, Tuple, Dict

import pandas as pd
import pandera as pa
from typing_extensions import final

from pandakeeper.node import Node
from pandakeeper.typing import PD_READ_PICKLE_ANNOTATION
from pandakeeper.validators import AnyDataFrame

__all__ = (
    'DataLoader',
    'StaticDataLoader',
    'DataFrameAdapter',
    'PickleLoader',
    'ExcelLoader'
)


class DataLoader(Node):
    __slots__ = ('__loader', '__loader_args', '__loader_kwargs')

    def __init__(self,
                 loader: Callable[..., pd.DataFrame],
                 *loader_args: Any,
                 output_validator: pa.DataFrameSchema,
                 **loader_kwargs: Any) -> None:
        super().__init__(output_validator=output_validator, already_cached=False)
        self.__loader = loader
        self.__loader_args = loader_args
        self.__loader_kwargs = loader_kwargs

    @final
    def _load_default(self) -> pd.DataFrame:
        return self.__loader(*self.__loader_args, **self.__loader_kwargs)

    @final
    @property
    def loader(self) -> Callable[..., pd.DataFrame]:
        return self.__loader

    @final
    @property
    def loader_args(self) -> Tuple[Any, ...]:
        return self.__loader_args

    @final
    @property
    def loader_kwargs(self) -> Dict[str, Any]:
        return self.__loader_kwargs


class StaticDataLoader(DataLoader):
    __slots__ = ()

    @final
    def _dump_to_cache(self, data: pd.DataFrame) -> None:
        pass

    @final
    def _load_cached(self) -> pd.DataFrame:
        return self._load_default()

    @final
    def _load_non_cached(self) -> pd.DataFrame:
        return self._load_default()

    @final
    def _clear_cache_storage(self) -> None:
        pass

    @property
    def use_cached(self) -> bool:
        return True

    @final
    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        return data


class DataFrameAdapter(StaticDataLoader):
    __slots__ = ()

    def __init__(self, df: pd.DataFrame, *, output_validator: pa.DataFrameSchema = AnyDataFrame) -> None:
        super().__init__(lambda: df, output_validator=output_validator)


class PickleLoader(StaticDataLoader):
    __slots__ = ()

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


class ExcelLoader(StaticDataLoader):
    __slots__ = ()

    def __init__(self,
                 *loader_args: Any,
                 output_validator: pa.DataFrameSchema,
                 **loader_kwargs: Any) -> None:
        super().__init__(pd.read_excel, *loader_args, **loader_kwargs, output_validator=output_validator)
