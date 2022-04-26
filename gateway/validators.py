from typing import Final

from pandera import DataFrameSchema

__all__ = (
    'AnyDataFrame',
)

AnyDataFrame: Final = DataFrameSchema()
