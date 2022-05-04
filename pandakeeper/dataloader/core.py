from collections.abc import Callable as _Callable
from types import MappingProxyType
from typing import Any, Optional, Callable, Tuple
from warnings import warn

import pandera as pa
from pandas import DataFrame, read_pickle, read_excel, read_csv
from typing_extensions import final

from pandakeeper.errors import check_type_compatibility
from pandakeeper.node import Node
from pandakeeper.typing import PD_READ_PICKLE_ANNOTATION
from pandakeeper.validators import AnyDataFrame

__all__ = (
    'DataLoader',
    'StaticDataLoader',
    'DataFrameAdapter',
    'PickleLoader',
    'ExcelLoader',
    'CsvLoader'
)


class DataLoader(Node):
    """
    Abstract class that defines an interface common to all data loaders,
    i.e. Nodes that can only generate data but not receive from other Nodes.
    """
    __slots__ = ('__loader', '__loader_args', '__loader_kwargs')

    def __init__(self,
                 loader: Callable[..., DataFrame],
                 *loader_args: Any,
                 **loader_kwargs: Any) -> None:
        """
        Abstract class that defines an interface common to all data loaders,
        i.e. Nodes that can only generate data but not receive from other Nodes.

        Args:
            loader:           loader function.
            *loader_args:     positional arguments passed to the loader function.
            **loader_kwargs:  keyword arguments passed to the loader function.
        """
        check_type_compatibility(loader, _Callable, 'Callable')  # type: ignore
        super().__init__(AnyDataFrame)
        self.__loader = loader
        self.__loader_args = loader_args
        self.__loader_kwargs = loader_kwargs

    @final
    def _load_default(self) -> DataFrame:
        """
        Returns the result of the loader function.

        Returns:
            Resulting DataFrame.
        """
        return self.__loader(*self.__loader_args, **self.__loader_kwargs)

    @final
    @property
    def loader(self) -> Callable[..., DataFrame]:
        """Loader function."""
        return self.__loader

    @final
    @property
    def loader_args(self) -> Tuple[Any, ...]:
        """Positional arguments of the loader function."""
        return self.__loader_args

    @final
    @property
    def loader_kwargs(self) -> MappingProxyType[str, Any]:
        """Keyword arguments of the loader function."""
        return MappingProxyType(self.__loader_kwargs)


class StaticDataLoader(DataLoader):
    """DataLoader base class for static data sources."""
    __slots__ = ()

    def _dump_to_cache(self, data: DataFrame) -> None:
        warn("'_dump_to_cache' does nothing for StaticDataLoader instances", RuntimeWarning)

    def _load_cached(self) -> DataFrame:
        warn(
            "'_load_cached' should not be called for StaticDataLoader instances. Switch to '_load_non_cached'",
            RuntimeWarning
        )
        return self._load_default()

    @final
    def _load_non_cached(self) -> DataFrame:
        return self._load_default()

    def _clear_cache_storage(self) -> None:
        warn("'_clear_cache_storage' does nothing for StaticDataLoader instances", RuntimeWarning)

    @property
    def use_cached(self) -> bool:
        return False

    @final
    def transform_data(self, data: DataFrame) -> DataFrame:
        return data


class DataFrameAdapter(StaticDataLoader):
    """DataLoader adapter for existing DataFrames."""
    __slots__ = ()

    def __init__(self,
                 df: DataFrame,
                 *,
                 output_validator: pa.DataFrameSchema = AnyDataFrame,
                 copy: bool = False) -> None:
        """
        DataLoader adapter for existing DataFrames.

        Args:
            df:                input DataFrame.
            output_validator:  output validator.
            copy:              whether to copy input DataFrame.
        """
        check_type_compatibility(df, DataFrame)
        check_type_compatibility(copy, bool)
        if copy:
            df = df.copy()
        super().__init__(lambda: df)
        self.set_output_validator(output_validator)


class PickleLoader(StaticDataLoader):
    """DataLoader that loads pickled DataFrames."""
    __slots__ = ()

    def __init__(self,
                 filepath_or_buffer: PD_READ_PICKLE_ANNOTATION,
                 compression: Optional[str] = 'infer',
                 *,
                 output_validator: pa.DataFrameSchema = AnyDataFrame) -> None:
        """
        DataLoader that loads pickled DataFrames.

        Args:
            filepath_or_buffer:  filepath or buffer to read pickle from.
            compression:         input file compression.
            output_validator:    output validator.
        """
        super().__init__(read_pickle, filepath_or_buffer, compression)
        self.set_output_validator(output_validator)


class ExcelLoader(StaticDataLoader):
    """DataLoader that loads Excel files."""
    __slots__ = ()

    def __init__(self,
                 *loader_args: Any,
                 output_validator: pa.DataFrameSchema = AnyDataFrame,
                 **loader_kwargs: Any) -> None:
        """
        DataLoader that loads Excel files.

        Args:
            *loader_args:      pandas.read_excel positional arguments.
            output_validator:  output validator.
            **loader_kwargs:   pandas.read_excel keyword arguments.
        """
        super().__init__(read_excel, *loader_args, **loader_kwargs)
        self.set_output_validator(output_validator)


class CsvLoader(StaticDataLoader):
    """DataLoader that loads csv-files."""
    __slots__ = ()

    def __init__(self,
                 *loader_args: Any,
                 output_validator: pa.DataFrameSchema = AnyDataFrame,
                 **loader_kwargs: Any) -> None:
        """
        DataLoader that loads csv-files.

        Args:
            *loader_args:      pandas.read_csv positional arguments.
            output_validator:  output validator.
            **loader_kwargs:   pandas.read_csv keyword arguments.
        """
        super().__init__(read_csv, *loader_args, **loader_kwargs)
        self.set_output_validator(output_validator)
