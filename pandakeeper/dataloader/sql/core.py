from collections.abc import Mapping as _Mapping, Callable as _Callable
from contextlib import ExitStack
from types import MappingProxyType
from typing import Callable, Any, Tuple, Mapping

import pandas as pd
import pandera as pa
from typing_extensions import final

from pandakeeper.dataloader.core import StaticDataLoader
from pandakeeper.errors import check_type_compatibility
from pandakeeper.utils import empty_mapping_proxy

__all__ = ('SqlLoader',)


class SqlLoader(StaticDataLoader):
    """DataLoader that loads data using SQL-connections."""
    __slots__ = ('__context_creator', '__read_sql_fn')

    def __init__(
            self,
            context_creator: Callable[..., Any],
            sql_query: str,
            *,
            context_creator_args: Tuple[Any, ...] = (),
            context_creator_kwargs: Mapping[str, Any] = empty_mapping_proxy,
            read_sql_fn: Callable[..., pd.DataFrame] = pd.read_sql,
            read_sql_args: Tuple[Any, ...] = (),
            read_sql_kwargs: Mapping[str, Any] = empty_mapping_proxy,
            output_validator: pa.DataFrameSchema) -> None:
        """
        DataLoader that loads data using SQL-connections.

        Args:
            context_creator:         callable that should create a SQL context and return a DB-connection.
            sql_query:               SQL-query to run.
            context_creator_args:    positional arguments for 'context_creator'.
            context_creator_kwargs:  keyword arguments for 'context_creator'.
            read_sql_fn:             function that creates pandas.DataFrame from the result of SQL-query.
            read_sql_args:           positional arguments for 'read_sql_fn'.
            read_sql_kwargs:         keyword arguments for 'read_sql_fn'.
            output_validator:        output validator.
        """
        check_type_compatibility(context_creator, _Callable, 'Callable')  # type: ignore
        check_type_compatibility(sql_query, str)
        check_type_compatibility(context_creator_args, tuple)
        check_type_compatibility(context_creator_kwargs, _Mapping, "dict or another Mapping")
        check_type_compatibility(read_sql_fn, _Callable, 'Callable')  # type: ignore
        check_type_compatibility(read_sql_args, tuple)
        check_type_compatibility(read_sql_kwargs, _Mapping, "dict or another Mapping")
        super().__init__(
            self.__load_sql,
            sql_query,
            context_creator_args,
            context_creator_kwargs,
            read_sql_args,
            read_sql_kwargs
        )
        self.set_output_validator(output_validator)
        self.__read_sql_fn = read_sql_fn
        self.__context_creator = context_creator

    @final
    def __load_sql(
            self,
            sql_query: str,
            context_creator_args: Tuple[Any, ...],
            context_creator_kwargs: Mapping[str, Any],
            read_sql_args: Tuple[Any, ...],
            read_sql_kwargs: Mapping[str, Any]) -> pd.DataFrame:
        """
        Builds necessary contexts and returns the result of the SQL-query.

        Args:
            sql_query:               SQL-query to run.
            context_creator_args:    positional arguments for 'context_creator'.
            context_creator_kwargs:  keyword arguments for 'context_creator'.
            read_sql_args:           positional arguments for 'read_sql_fn'.
            read_sql_kwargs:         keyword arguments for 'read_sql_fn'.

        Returns:
            Resulting DataFrame.
        """
        with ExitStack() as exit_stack:
            conn = self.__context_creator(exit_stack, *context_creator_args, **context_creator_kwargs)
            return self.__read_sql_fn(sql_query, conn, *read_sql_args, **read_sql_kwargs)

    @final
    @property
    def read_sql_fn(self) -> Callable[..., pd.DataFrame]:
        """Function that creates pandas.DataFrame from the result of SQL-query."""
        return self.__read_sql_fn

    @final
    @property
    def context_creator(self) -> Callable[..., Any]:
        """Callable that create SQL context and return the DB-connection."""
        return self.__context_creator

    @final
    @property
    def sql_query(self) -> str:
        """SQL-query to run."""
        return self.loader_args[0]

    @final
    @property
    def context_creator_args(self) -> Tuple[Any, ...]:
        """Positional arguments for 'sql_context_creator'."""
        return self.loader_args[1]

    @final
    @property
    def context_creator_kwargs(self) -> MappingProxyType[str, Any]:
        """Keyword arguments for 'sql_context_creator'."""
        return MappingProxyType(self.loader_args[2])

    @final
    @property
    def read_sql_args(self) -> Tuple[Any, ...]:
        """Positional arguments for 'read_sql_fn'."""
        return self.loader_args[3]

    @final
    @property
    def read_sql_kwargs(self) -> MappingProxyType[str, Any]:
        """Keyword arguments for 'read_sql_fn'."""
        return MappingProxyType(self.loader_args[4])
