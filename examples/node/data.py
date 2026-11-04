"""Sample data for the node example."""

import typing as t

from sqlalchemy.orm import Session

from .models import Node


def seed(session: Session) -> None:
    nodes: t.List[Node] = [
        Node(id=1, name="Node A"),
        Node(id=2, name="Node B"),
        Node(id=3, name="Node C"),
        Node(id=4, name="Node A_A", node_id=1),
        Node(id=5, name="Node B_B", node_id=2),
        Node(id=6, name="Node C_C", node_id=3),
        Node(id=7, name="Node A_A_A", node_id=4),
        Node(id=8, name="Node B_B_B", node_id=5),
        Node(id=9, name="Node C_C_C", node_id=6),
    ]
    session.add_all(nodes)
