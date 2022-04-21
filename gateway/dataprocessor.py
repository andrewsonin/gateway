from typing import Optional, Union, Dict, List

import pandas as pd
import pandera as pa

from gateway.node import Node
from gateway.validators import AnyDataFrame


class NodeConnection:
    __slots__ = ('__node', '__input_validator')

    def __init__(self, node: Node, input_validator: pa.DataFrameSchema = AnyDataFrame) -> None:
        self.__node = node
        self.__input_validator = input_validator

    @property
    def node(self) -> Node:
        return self.__node

    @property
    def input_validator(self) -> pa.DataFrameSchema:
        return self.__input_validator

    def extract_data(self) -> pd.DataFrame:
        data = self.__node.extract_data()
        return self.__input_validator.validate(data)


class DataProcessor(Node):
    __slots__ = ('__positional_node_connections', '__named_node_connections')

    def __init__(self, *, already_cached: bool, output_validator: pa.DataFrameSchema = AnyDataFrame):
        super().__init__(already_cached=already_cached, output_validator=output_validator)
        self.__positional_node_connections: List[NodeConnection] = []
        self.__named_node_connections: Dict[str, NodeConnection] = {}

    @property
    def positional_node_connections(self) -> List[NodeConnection]:
        return self.__positional_node_connections.copy()

    @property
    def named_node_connections(self) -> Dict[str, NodeConnection]:
        return self.__named_node_connections.copy()

    def connect_parental_node(self, node: Union[Node, NodeConnection], keyword: Optional[str] = None) -> None:
        if isinstance(node, Node):
            node = NodeConnection(node)
        if keyword is None:
            self.__positional_node_connections.append(node)
        else:
            named_node_connections = self.__named_node_connections
            if keyword in named_node_connections:
                raise KeyError(f"Duplicate name '{keyword}' for named NodeConnection")
            named_node_connections[keyword] = node

    def connect_parental_nodes(self,
                               *positional_nodes: Union[Node, NodeConnection],
                               **keyword_nodes: Union[Node, NodeConnection]) -> None:
        for node in positional_nodes:
            self.connect_parental_node(node)
        for keyword, node in keyword_nodes.items():
            self.connect_parental_node(node, keyword)
