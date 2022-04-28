from typing import Callable, Any

import pandas as pd
import pandera as pa

from ..core import StaticDataLoader

__all__ = ('SqlLoader',)


class SqlLoader(StaticDataLoader):
    __slots__ = ('__sql_context_creator', '__context_processor')

    def __init__(self,
                 sql_context_creator: Callable[..., Any],
                 sql_query: str,
                 *loader_args: Any,
                 output_validator: pa.DataFrameSchema,
                 context_processor: Callable[[Any], Any] = lambda x: x,
                 **loader_kwargs: Any) -> None:
        super().__init__(self._load_sql, sql_query, *loader_args, **loader_kwargs, output_validator=output_validator)
        self.__sql_context_creator = sql_context_creator
        self.__context_processor = context_processor

    def _load_sql(self, sql_query: str, *loader_args: Any, **loader_kwargs: Any) -> pd.DataFrame:
        with self.__sql_context_creator() as context:
            conn = self.__context_processor(context)
            return pd.read_sql(sql_query, conn, *loader_args, **loader_kwargs)
