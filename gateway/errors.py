class GatewayError(Exception):
    __slots__ = ()


class GraphIsLoopedError(GatewayError):
    __slots__ = ()
