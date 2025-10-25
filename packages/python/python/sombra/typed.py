from typing import Any, Generic, TypeVar, overload, TYPE_CHECKING

if TYPE_CHECKING:
    from typing_extensions import TypedDict
    from .sombra import (
        SombraDB as NativeSombraDB,
        SombraTransaction as NativeSombraTransaction,
        SombraNode,
        SombraEdge,
        BfsResult,
        QueryBuilder as NativeQueryBuilder,
    )
else:
    try:
        from typing_extensions import TypedDict
    except ImportError:
        from typing import TypedDict
    
    from .sombra import (
        SombraDB as NativeSombraDB,
        SombraTransaction as NativeSombraTransaction,
        SombraNode,
        SombraEdge,
        BfsResult,
        QueryBuilder as NativeQueryBuilder,
    )

PropertyValue = bool | int | float | str | bytes

SchemaT = TypeVar('SchemaT', bound='GraphSchema')


class GraphSchema(TypedDict):
    nodes: dict[str, dict[str, PropertyValue]]
    edges: dict[str, Any]


class TypedQueryBuilder(Generic[SchemaT]):
    def __init__(self, db: 'SombraDB[SchemaT]', builder: 'NativeQueryBuilder'):
        self._db = db
        self._builder = builder

    def start_from(self, node_ids: list[int]) -> 'TypedQueryBuilder[SchemaT]':
        self._builder.start_from(node_ids)
        return self

    def start_from_label(self, label: str) -> 'TypedQueryBuilder[SchemaT]':
        self._builder.start_from_label(label)
        return self

    def start_from_property(
        self,
        label: str,
        key: str,
        value: PropertyValue
    ) -> 'TypedQueryBuilder[SchemaT]':
        self._builder.start_from_property(label, key, value)
        return self

    def traverse(
        self,
        edge_types: list[str],
        direction: str,
        depth: int
    ) -> 'TypedQueryBuilder[SchemaT]':
        self._builder.traverse(edge_types, direction, depth)
        return self

    def limit(self, n: int) -> 'TypedQueryBuilder[SchemaT]':
        self._builder.limit(n)
        return self

    def execute(self) -> Any:
        return self._builder.execute()


class SombraTransaction(Generic[SchemaT]):
    def __init__(self, tx: 'NativeSombraTransaction'):
        self._tx = tx

    def id(self) -> int:
        return self._tx.id()

    @overload
    def add_node(self, labels: str, properties: dict[str, PropertyValue]) -> int: ...
    
    @overload
    def add_node(self, labels: list[str], properties: dict[str, PropertyValue]) -> int: ...

    def add_node(
        self,
        labels: str | list[str],
        properties: dict[str, PropertyValue]
    ) -> int:
        if isinstance(labels, str):
            labels = [labels]
        return self._tx.add_node(labels, properties)

    def add_edge(
        self,
        source_node_id: int,
        target_node_id: int,
        edge_type: str,
        properties: dict[str, PropertyValue] | None = None
    ) -> int:
        return self._tx.add_edge(
            source_node_id,
            target_node_id,
            edge_type,
            properties or {}
        )

    def get_node(self, node_id: int) -> 'SombraNode':
        return self._tx.get_node(node_id)

    def get_edge(self, edge_id: int) -> 'SombraEdge':
        return self._tx.get_edge(edge_id)

    def get_neighbors(self, node_id: int) -> list[int]:
        return self._tx.get_neighbors(node_id)

    def get_outgoing_edges(self, node_id: int) -> list[int]:
        return self._tx.get_outgoing_edges(node_id)

    def get_incoming_edges(self, node_id: int) -> list[int]:
        return self._tx.get_incoming_edges(node_id)

    def delete_node(self, node_id: int) -> None:
        self._tx.delete_node(node_id)

    def delete_edge(self, edge_id: int) -> None:
        self._tx.delete_edge(edge_id)

    def set_node_property(
        self,
        node_id: int,
        key: str,
        value: PropertyValue
    ) -> None:
        self._tx.set_node_property(node_id, key, value)

    def remove_node_property(self, node_id: int, key: str) -> None:
        self._tx.remove_node_property(node_id, key)

    def commit(self) -> None:
        self._tx.commit()

    def rollback(self) -> None:
        self._tx.rollback()

    def get_incoming_neighbors(self, node_id: int) -> list[int]:
        return self._tx.get_incoming_neighbors(node_id)

    def get_neighbors_two_hops(self, node_id: int) -> list[int]:
        return self._tx.get_neighbors_two_hops(node_id)

    def get_neighbors_three_hops(self, node_id: int) -> list[int]:
        return self._tx.get_neighbors_three_hops(node_id)

    def bfs_traversal(self, start_node_id: int, max_depth: int) -> list['BfsResult']:
        return self._tx.bfs_traversal(start_node_id, max_depth)

    def get_nodes_by_label(self, label: str) -> list[int]:
        return self._tx.get_nodes_by_label(label)

    def get_nodes_in_range(self, start: int, end: int) -> list[int]:
        return self._tx.get_nodes_in_range(start, end)

    def get_nodes_from(self, start: int) -> list[int]:
        return self._tx.get_nodes_from(start)

    def get_nodes_to(self, end: int) -> list[int]:
        return self._tx.get_nodes_to(end)

    def get_first_node(self) -> int | None:
        return self._tx.get_first_node()

    def get_last_node(self) -> int | None:
        return self._tx.get_last_node()

    def get_first_n_nodes(self, n: int) -> list[int]:
        return self._tx.get_first_n_nodes(n)

    def get_last_n_nodes(self, n: int) -> list[int]:
        return self._tx.get_last_n_nodes(n)

    def get_all_node_ids_ordered(self) -> list[int]:
        return self._tx.get_all_node_ids_ordered()

    def count_outgoing_edges(self, node_id: int) -> int:
        return self._tx.count_outgoing_edges(node_id)

    def count_incoming_edges(self, node_id: int) -> int:
        return self._tx.count_incoming_edges(node_id)


class SombraDB(Generic[SchemaT]):
    def __init__(self, path: str):
        self._db = NativeSombraDB(path)

    def begin_transaction(self) -> SombraTransaction[SchemaT]:
        return SombraTransaction(self._db.begin_transaction())

    @overload
    def add_node(self, labels: str, properties: dict[str, PropertyValue]) -> int: ...
    
    @overload
    def add_node(self, labels: list[str], properties: dict[str, PropertyValue]) -> int: ...

    def add_node(
        self,
        labels: str | list[str],
        properties: dict[str, PropertyValue]
    ) -> int:
        if isinstance(labels, str):
            labels = [labels]
        return self._db.add_node(labels, properties)

    def add_edge(
        self,
        source_node_id: int,
        target_node_id: int,
        edge_type: str,
        properties: dict[str, PropertyValue] | None = None
    ) -> int:
        return self._db.add_edge(
            source_node_id,
            target_node_id,
            edge_type,
            properties or {}
        )

    def get_node(self, node_id: int) -> 'SombraNode':
        return self._db.get_node(node_id)

    def get_edge(self, edge_id: int) -> 'SombraEdge':
        return self._db.get_edge(edge_id)

    def get_neighbors(self, node_id: int) -> list[int]:
        return self._db.get_neighbors(node_id)

    def get_outgoing_edges(self, node_id: int) -> list[int]:
        return self._db.get_outgoing_edges(node_id)

    def get_incoming_edges(self, node_id: int) -> list[int]:
        return self._db.get_incoming_edges(node_id)

    def delete_node(self, node_id: int) -> None:
        self._db.delete_node(node_id)

    def delete_edge(self, edge_id: int) -> None:
        self._db.delete_edge(edge_id)

    def set_node_property(
        self,
        node_id: int,
        key: str,
        value: PropertyValue
    ) -> None:
        self._db.set_node_property(node_id, key, value)

    def remove_node_property(self, node_id: int, key: str) -> None:
        self._db.remove_node_property(node_id, key)

    def flush(self) -> None:
        self._db.flush()

    def checkpoint(self) -> None:
        self._db.checkpoint()

    def get_incoming_neighbors(self, node_id: int) -> list[int]:
        return self._db.get_incoming_neighbors(node_id)

    def get_neighbors_two_hops(self, node_id: int) -> list[int]:
        return self._db.get_neighbors_two_hops(node_id)

    def get_neighbors_three_hops(self, node_id: int) -> list[int]:
        return self._db.get_neighbors_three_hops(node_id)

    def bfs_traversal(self, start_node_id: int, max_depth: int) -> list['BfsResult']:
        return self._db.bfs_traversal(start_node_id, max_depth)

    def get_nodes_by_label(self, label: str) -> list[int]:
        return self._db.get_nodes_by_label(label)

    def get_nodes_in_range(self, start: int, end: int) -> list[int]:
        return self._db.get_nodes_in_range(start, end)

    def get_nodes_from(self, start: int) -> list[int]:
        return self._db.get_nodes_from(start)

    def get_nodes_to(self, end: int) -> list[int]:
        return self._db.get_nodes_to(end)

    def get_first_node(self) -> int | None:
        return self._db.get_first_node()

    def get_last_node(self) -> int | None:
        return self._db.get_last_node()

    def get_first_n_nodes(self, n: int) -> list[int]:
        return self._db.get_first_n_nodes(n)

    def get_last_n_nodes(self, n: int) -> list[int]:
        return self._db.get_last_n_nodes(n)

    def get_all_node_ids_ordered(self) -> list[int]:
        return self._db.get_all_node_ids_ordered()

    def count_outgoing_edges(self, node_id: int) -> int:
        return self._db.count_outgoing_edges(node_id)

    def count_incoming_edges(self, node_id: int) -> int:
        return self._db.count_incoming_edges(node_id)

    def query(self) -> TypedQueryBuilder[SchemaT]:
        return TypedQueryBuilder(self, self._db.query())


__all__ = [
    'SombraDB',
    'SombraTransaction',
    'TypedQueryBuilder',
    'GraphSchema',
    'PropertyValue',
]
