from types import MappingProxyType
from typing import Mapping, Any, Tuple

empty_mapping_proxy: Mapping[str, Any] = MappingProxyType({})


def pass_through_any(*args: Any) -> Tuple[Any, ...]:
    return args


def pass_through_one(arg: Any) -> Any:
    return arg
