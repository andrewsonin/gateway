from collections.abc import Mapping as _Mapping, Callable as _Callable
from typing import Callable, Any, Tuple, Mapping, Dict

import pandas as pd
import pandera as pa
from typing_extensions import final

from pandakeeper.dataloader.core import StaticDataLoader
from pandakeeper.errors import check_type_compatibility
from pandakeeper.utils import empty_mapping_proxy, pass_through_one

__all__ = ('SqlLoader',)


class SqlLoader(StaticDataLoader):
    """DataLoader that loads data using SQL-connections."""
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
        """
        DataLoader that loads data using SQL-connections.

        Args:
            sql_context_creator:       callable that creates SQL context returning the DB-connection.
            sql_query:                 SQL-query to run.
            read_sql_fn:               function that creates pandas.DataFrame from the result of SQL-query.
            read_sql_args:             positional arguments for 'read_sql_fn'.
            read_sql_kwargs:           keyword arguments for 'read_sql_fn'.
            output_validator:          output validator.
            context_creator_args:      positional arguments for 'sql_context_creator'.
            context_creator_kwargs:    keyword arguments for 'sql_context_creator'.
            context_processor:         function that can additionally process the DB-connection.
            context_processor_args:    positional arguments for 'context_processor'.
            context_processor_kwargs:  keyword arguments for 'context_processor'.
        """
        check_type_compatibility(sql_context_creator, _Callable, 'Callable')  # type: ignore
        check_type_compatibility(sql_query, str)
        check_type_compatibility(read_sql_fn, _Callable, 'Callable')  # type: ignore
        check_type_compatibility(read_sql_args, tuple)
        check_type_compatibility(read_sql_kwargs, _Mapping, "dict or another Mapping")
        check_type_compatibility(context_creator_args, tuple)
        check_type_compatibility(context_creator_kwargs, _Mapping, "dict or another Mapping")
        check_type_compatibility(context_processor, _Callable, 'Callable')  # type: ignore
        check_type_compatibility(context_processor_args, tuple)
        check_type_compatibility(context_processor_kwargs, _Mapping, "dict or another Mapping")
        super().__init__(
            self.__load_sql,
            sql_query,
            read_sql_args,
            read_sql_kwargs,
            context_creator_args,
            context_creator_kwargs,
            context_processor_args,
            context_processor_kwargs
        )
        self.set_output_validator(output_validator)
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
        """
        Builds necessary contexts and returns the result of the SQL-query.

        Args:
            sql_query:                 SQL-query to run.
            read_sql_args:             positional arguments for 'read_sql_fn'.
            read_sql_kwargs:           keyword arguments for 'read_sql_fn'.
            context_creator_args:      positional arguments for 'sql_context_creator'.
            context_creator_kwargs:    keyword arguments for 'sql_context_creator'.
            context_processor_args:    positional arguments for 'context_processor'.
            context_processor_kwargs:  keyword arguments for 'context_processor'.

        Returns:
            Resulting DataFrame.
        """
        with self.__sql_context_creator(*context_creator_args, **context_creator_kwargs) as context:
            conn = self.__context_processor(context, *context_processor_args, **context_processor_kwargs)
            return self.__read_sql_fn(sql_query, conn, *read_sql_args, **read_sql_kwargs)

    @final
    @property
    def read_sql_fn(self) -> Callable[..., pd.DataFrame]:
        """Function that creates pandas.DataFrame from the result of SQL-query."""
        return self.__read_sql_fn

    @final
    @property
    def sql_query(self) -> str:
        """SQL-query to run."""
        return self.loader_args[0]

    @final
    @property
    def read_sql_args(self) -> Tuple[Any, ...]:
        """Positional arguments for 'read_sql_fn'."""
        return self.loader_args[1]

    @final
    @property
    def read_sql_kwargs(self) -> Dict[str, Any]:
        """Keyword arguments for 'read_sql_fn'."""
        return dict(self.loader_args[2])

    @final
    @property
    def sql_context_creator(self) -> Callable[..., Any]:
        """Callable that creates SQL context returning the DB-connection."""
        return self.__sql_context_creator

    @final
    @property
    def context_processor(self) -> Callable[..., Any]:
        """Function that can additionally process the DB-connection."""
        return self.__context_processor

    @final
    @property
    def context_creator_args(self) -> Tuple[Any, ...]:
        """Positional arguments for 'sql_context_creator'."""
        return self.loader_args[3]

    @final
    @property
    def context_creator_kwargs(self) -> Dict[str, Any]:
        """Keyword arguments for 'sql_context_creator'."""
        return dict(self.loader_args[4])

    @final
    @property
    def context_processor_args(self) -> Tuple[Any, ...]:
        """Positional arguments for 'context_processor'."""
        return self.loader_args[5]

    @final
    @property
    def context_processor_kwargs(self) -> Dict[str, Any]:
        """Keyword arguments for 'context_processor'."""
        return dict(self.loader_args[6])
