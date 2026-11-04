"""Sample data for the schemas example."""

from sqlalchemy.orm import Session

from .models import Child, Parent


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
            Child(id=1, name="Child A Parent A", parent_id=1),
            Child(id=2, name="Child B Parent A", parent_id=2),
            Child(id=3, name="Child C Parent B", parent_id=3),
        ]
    )
