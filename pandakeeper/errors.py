from typing import Type, Optional, Any

from varname import argname

__all__ = (
    'GatewayError',
    'LoopedGraphError',
    'check_type_compatibility'
)


class GatewayError(Exception):
    """Base class for library-specific exceptions."""
    __slots__ = ()


class LoopedGraphError(GatewayError):
    """Throws when the logic of the algorithms can be violated by the presence of loops in graphs."""
    __slots__ = ()


def check_type_compatibility(var: Any, expected_type: Type, expected_msg: Optional[str] = None) -> None:
    """
    Checks type compatibility between variable and expected type.

    Args:
        var:            variable ot attribute to check.
        expected_type:  expected type.
        expected_msg:   error message suffix.
    """
    if not isinstance(var, expected_type):
        if expected_msg is None:
            expected_msg = expected_type.__name__
            type_module = expected_type.__module__
            if type_module != 'builtins':
                expected_msg = f"{type_module}.{expected_msg}"
        raise TypeError(f"Incompatible type for argument '{argname('var')}': {type(var)}. Expected: {expected_msg}")
