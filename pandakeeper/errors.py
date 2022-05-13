from typing import Tuple, Type, Optional, Union, Any

from varname import argname

from pandakeeper.utils import get_fully_qualified_name, get_fully_qualified_names

__all__ = (
    'PandakeeperError',
    'LoopedGraphError',
    'IncompatibleTypeError',
    'check_type_compatibility'
)


class PandakeeperError(Exception):
    """Base class for library-specific exceptions."""
    __slots__ = ()


class LoopedGraphError(PandakeeperError):
    """Throws when the logic of the algorithms can be violated by the presence of loops in graphs."""
    __slots__ = ()


class IncompatibleTypeError(TypeError, PandakeeperError):
    """Throws when the type is incompatible with the expected logic."""
    __slots__ = ()


def check_type_compatibility(var: Any, expected_type: Union[Type, Tuple[Type, ...]],
                             expected_msg: Optional[str] = None) -> None:
    """
    Checks type compatibility between variable and expected type.

    Args:
        var:            variable ot attribute to check.
        expected_type:  expected type.
        expected_msg:   error message suffix.
    """
    if not isinstance(expected_type, tuple):
        expected_type = (expected_type,)

    if not isinstance(var, expected_type):
        if expected_msg is None:
            expected_msg = ', '.join(get_fully_qualified_names(expected_type))
            if len(expected_type) != 1:
                expected_msg = f'[{expected_msg}]'
        typename = get_fully_qualified_name(type(var))
        raise IncompatibleTypeError(
            f"Incompatible type for argument '{argname('var')}': "
            f"{typename}. Expected: {expected_msg}"
        )
