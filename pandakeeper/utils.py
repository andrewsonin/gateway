from functools import partial
from types import MappingProxyType
from typing import Callable, Iterable, Iterator, Mapping, Type, Any, Tuple

__all__ = (
    'empty_mapping_proxy',
    'pass_through_any',
    'pass_through_one',
    'get_fully_qualified_name',
    'get_fully_qualified_names'
)

empty_mapping_proxy: Mapping[str, Any] = MappingProxyType({})


def pass_through_any(*args: Any) -> Tuple[Any, ...]:
    return args


def pass_through_one(arg: Any) -> Any:
    return arg


def get_fully_qualified_name(typ: Type) -> str:
    name = typ.__name__
    type_module = typ.__module__
    if type_module != 'builtins':
        name = f"{type_module}.{name}"
    return name


get_fully_qualified_names: Callable[[Iterable[Type]], Iterator[str]] = partial(  # type: ignore
    map,
    get_fully_qualified_name
)
