from itertools import chain, repeat
from typing import Optional, Union, Dict, List, Tuple, Iterator, Iterable

import pandas as pd
import pandera as pa
from typing_extensions import final

from pandakeeper.node import Node
from pandakeeper.validators import AnyDataFrame

__all__ = (
    'NodeConnection',
    'DataProcessor'
)


class NodeConnection:
    __slots__ = ('__node', '__input_validator')

    def __init__(self, node: Node, input_validator: pa.DataFrameSchema = AnyDataFrame) -> None:
        self.__node = node
        self.__input_validator = input_validator

    @final
    @property
    def node(self) -> Node:
        return self.__node

    @final
    @property
    def input_validator(self) -> pa.DataFrameSchema:
        return self.__input_validator

    @final
    def extract_data(self) -> pd.DataFrame:
        data = self.__node.extract_data()
        return self.__input_validator.validate(data)


class DataProcessor(Node):
    __slots__ = ('__positional_node_connections', '__named_node_connections')

    def __init__(self, *, already_cached: bool, output_validator: pa.DataFrameSchema = AnyDataFrame):
        super().__init__(already_cached=already_cached, output_validator=output_validator)
        self.__positional_node_connections: List[NodeConnection] = []
        self.__named_node_connections: Dict[str, NodeConnection] = {}

    @final
    @property
    def positional_node_connections(self) -> List[NodeConnection]:
        return self.__positional_node_connections.copy()

    @final
    @property
    def named_node_connections(self) -> Dict[str, NodeConnection]:
        return self.__named_node_connections.copy()

    @final
    def __connect_parental_node_body(self,
                                     node_connection: NodeConnection,
                                     keyword: Optional[str] = None) -> None:
        if keyword is None:
            self.__positional_node_connections.append(node_connection)
        else:
            named_node_connections = self.__named_node_connections
            if keyword in named_node_connections:
                raise KeyError(f"Duplicate name '{keyword}' for named NodeConnection")
            named_node_connections[keyword] = node_connection
        self._add_edge_to_connection_graph(node_connection.node)

    @final
    def connect_parental_node(self,
                              node_connection: Union[Node, NodeConnection],
                              keyword: Optional[str] = None) -> None:
        self.drop_cache()
        self.__connect_parental_node_body(*_check_node_connection(node_connection, keyword))

    @final
    def connect_parental_nodes(self,
                               *positional_nodes: Union[Node, NodeConnection],
                               **keyword_nodes: Union[Node, NodeConnection]) -> None:
        if positional_nodes or keyword_nodes:
            self.drop_cache()
            nodes = tuple(
                _check_node_connections(
                    chain(
                        zip(repeat(None), positional_nodes),
                        keyword_nodes.items()
                    )
                )
            )
            for args in nodes:
                self.__connect_parental_node_body(*args)


def _check_node_connection(node: Union[Node, NodeConnection],
                           keyword: Optional[str],
                           arg_position: int = 0) -> Tuple[NodeConnection, Optional[str]]:
    if isinstance(node, Node):
        return NodeConnection(node), keyword
    elif isinstance(node, NodeConnection):
        return node, keyword
    if keyword is None:
        err_msg = f"Positional argument in position {arg_position} has incompatible type: {type(node)}"
    else:
        err_msg = f"Argument '{keyword}' has incompatible type: {type(node)}"
    raise TypeError(err_msg)


def _check_node_connections(nodes: Iterable[Tuple[Optional[str], Union[Node, NodeConnection]]]) -> Iterator[
    Tuple[NodeConnection, Optional[str]]
]:
    for i, (keyword, node) in enumerate(nodes):
        yield _check_node_connection(node, keyword, i)
