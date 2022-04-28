from typing import Callable, Any, Tuple, Mapping, Dict

import pandas as pd
import pandera as pa
from typing_extensions import final

from pandakeeper.dataloader.core import StaticDataLoader
from pandakeeper.utils import empty_mapping_proxy, pass_through_one

__all__ = ('SqlLoader',)


class SqlLoader(StaticDataLoader):
    __slots__ = ('__read_sql_fn', '__sql_context_creator', '__context_processor')

    def __init__(
            self,
            sql_context_creator: Callable[..., Any],
            sql_query: str,
            *,
            read_sql_fn: Callable[..., pd.DataFrame] = pd.read_sql,
            read_sql_args: Tuple[Any, ...] = (),
            read_sql_kwargs: Mapping[str, Any] = empty_mapping_proxy,
            output_validator: pa.DataFrameSchema,
            context_creator_args: Tuple[Any, ...] = (),
            context_creator_kwargs: Mapping[str, Any] = empty_mapping_proxy,
            context_processor: Callable[..., Any] = pass_through_one,
            context_processor_args: Tuple[Any, ...] = (),
            context_processor_kwargs: Mapping[str, Any] = empty_mapping_proxy) -> None:
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
        self.__read_sql_fn = read_sql_fn
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
            return self.__read_sql_fn(sql_query, conn, *read_sql_args, **read_sql_kwargs)

    @final
    @property
    def read_sql_fn(self) -> Callable[..., pd.DataFrame]:
        return self.__read_sql_fn

    @final
    @property
    def sql_query(self) -> str:
        return self.loader_args[0]

    @final
    @property
    def read_sql_args(self) -> Tuple[Any, ...]:
        return self.loader_args[1]

    @final
    @property
    def read_sql_kwargs(self) -> Dict[str, Any]:
        return dict(self.loader_args[2])

    @final
    @property
    def sql_context_creator(self) -> Callable[..., Any]:
        return self.__sql_context_creator

    @final
    @property
    def context_processor(self) -> Callable[..., Any]:
        return self.__context_processor

    @final
    @property
    def context_creator_args(self) -> Tuple[Any, ...]:
        return self.loader_args[3]

    @final
    @property
    def context_creator_kwargs(self) -> Dict[str, Any]:
        return dict(self.loader_args[4])

    @final
    @property
    def context_processor_args(self) -> Tuple[Any, ...]:
        return self.loader_args[5]

    @final
    @property
    def context_processor_kwargs(self) -> Dict[str, Any]:
        return dict(self.loader_args[6])
