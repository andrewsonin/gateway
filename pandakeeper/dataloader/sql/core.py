from types import MappingProxyType
from typing import Callable, Any, Tuple, Mapping

import pandas as pd
import pandera as pa
from typing_extensions import final

from ..core import StaticDataLoader

__all__ = ('SqlLoader',)

_empty_mapping_proxy: Mapping[str, Any] = MappingProxyType({})


class SqlLoader(StaticDataLoader):
    __slots__ = ('__sql_context_creator', '__context_processor')

    def __init__(
            self,
            sql_context_creator: Callable[..., Any],
            sql_query: str,
            *,
            read_sql_args: Tuple[Any, ...] = (),
            read_sql_kwargs: Mapping[str, Any] = _empty_mapping_proxy,
            output_validator: pa.DataFrameSchema,
            context_creator_args: Tuple[Any, ...] = (),
            context_creator_kwargs: Mapping[str, Any] = _empty_mapping_proxy,
            context_processor: Callable[..., Any] = lambda x: x,
            context_processor_args: Tuple[Any, ...] = (),
            context_processor_kwargs: Mapping[str, Any] = _empty_mapping_proxy) -> None:
        super().__init__(
            self.__load_sql,
            sql_query,
            read_sql_args,
            read_sql_kwargs,
            context_creator_args,
            context_creator_kwargs,
            context_processor_args,
            context_processor_kwargs,
            output_validator=output_validator
        )
        self.__sql_context_creator = sql_context_creator
        self.__context_processor = context_processor

    @final
    def __load_sql(
            self,
            sql_query: str,
            read_sql_args: Tuple[Any, ...],
            read_sql_kwargs: Mapping[str, Any],
            context_creator_args: Tuple[Any, ...],
            context_creator_kwargs: Mapping[str, Any],
            context_processor_args: Tuple[Any, ...],
            context_processor_kwargs: Mapping[str, Any]) -> pd.DataFrame:
        with self.__sql_context_creator(*context_creator_args, **context_creator_kwargs) as context:
            conn = self.__context_processor(context, *context_processor_args, **context_processor_kwargs)
            return pd.read_sql(sql_query, conn, *read_sql_args, **read_sql_kwargs)
