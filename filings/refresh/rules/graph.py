"""Connected components, the one graph algorithm the normalization needs.

The SQL walked edges with a recursive CTE and took ``MIN(group_id)`` per
node; union-find gives the same partition with the same representative.
"""

from __future__ import annotations

from typing import Hashable, Iterable, TypeVar

import polars as pl

Node = TypeVar("Node", bound=Hashable)


class DisjointSet:
    """Union-find whose representative is the smallest member, as ``MIN()`` gives."""

    def __init__(self) -> None:
        self._parent: dict[Hashable, Hashable] = {}

    def add(self, node: Hashable) -> None:
        self._parent.setdefault(node, node)

    def find(self, node: Hashable) -> Hashable:
        self.add(node)
        root = node
        while self._parent[root] != root:
            root = self._parent[root]
        while self._parent[node] != root:
            self._parent[node], node = root, self._parent[node]
        return root

    def union(self, a: Hashable, b: Hashable) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        low, high = (ra, rb) if ra < rb else (rb, ra)  # type: ignore[operator]
        self._parent[high] = low


def components(
    nodes: Iterable[Node], edges: Iterable[tuple[Node, Node]]
) -> dict[Node, Node]:
    """Map every node to the smallest node of its connected component."""
    forest = DisjointSet()
    for node in nodes:
        forest.add(node)
    for a, b in edges:
        forest.union(a, b)
    return {node: forest.find(node) for node in list(forest._parent)}  # type: ignore[misc]


def component_frame(
    nodes: pl.Series, src: pl.Series, dst: pl.Series, node: str, component: str
) -> pl.DataFrame:
    """:func:`components` as a two-column frame ``node -> component``."""
    mapping = components(nodes.to_list(), zip(src.to_list(), dst.to_list()))
    return pl.DataFrame(
        {node: list(mapping.keys()), component: list(mapping.values())},
        schema={node: nodes.dtype, component: nodes.dtype},
    )
