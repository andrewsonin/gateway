__all__ = (
    'GatewayError',
    'LoopedGraphError'
)


class GatewayError(Exception):
    __slots__ = ()


class LoopedGraphError(GatewayError):
    __slots__ = ()
