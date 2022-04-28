from types import MappingProxyType
from typing import Mapping, Any, Tuple

__all__ = (
    'empty_mapping_proxy',
    'pass_through_any',
    'pass_through_one'
)

empty_mapping_proxy: Mapping[str, Any] = MappingProxyType({})


def pass_through_any(*args: Any) -> Tuple[Any, ...]:
    return args


def pass_through_one(arg: Any) -> Any:
    return arg
