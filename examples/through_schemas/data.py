"""Sample data for the through_schemas example."""

from sqlalchemy.orm import Session

from .models import Child, ChildToParent, Parent


def seed(session: Session) -> None:
    session.add_all(
        [
            Parent(id=1, name="Parent A"),
            Parent(id=2, name="Parent B"),
            Parent(id=3, name="Parent C"),
        ]
    )
    session.flush()
    session.add_all(
        [
            Child(id=1, name="Child A Parent A"),
            Child(id=2, name="Child B Parent A"),
            Child(id=3, name="Child C Parent B"),
        ]
    )
    session.flush()
    session.add_all(
        [
            ChildToParent(name="Relation 1", child_id=1, parent_id=1),
            ChildToParent(name="Relation 2", child_id=2, parent_id=1),
            ChildToParent(name="Relation 3", child_id=3, parent_id=2),
        ]
    )
