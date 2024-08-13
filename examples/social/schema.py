import click
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.schema import UniqueConstraint

from pgsync.base import create_database, pg_engine
from pgsync.helper import teardown
from pgsync.utils import config_loader, get_config


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user"
    __table_args__ = (UniqueConstraint("name"),)
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    name: Mapped[str] = mapped_column(sa.String, nullable=False)
    age: Mapped[int] = mapped_column(sa.Integer, nullable=True)
    gender: Mapped[str] = mapped_column(sa.String, nullable=True)


class Post(Base):
    __tablename__ = "post"
    __table_args__ = ()
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    title: Mapped[str] = mapped_column(sa.String, nullable=False)
    slug: Mapped[str] = mapped_column(sa.String, nullable=True)


class Comment(Base):
    __tablename__ = "comment"
    __table_args__ = ()
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    title: Mapped[str] = mapped_column(sa.String, nullable=True)
    content: Mapped[str] = mapped_column(sa.String, nullable=True)


class Tag(Base):
    __tablename__ = "tag"
    __table_args__ = (UniqueConstraint("name"),)
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    name: Mapped[str] = mapped_column(sa.String, nullable=False)


class UserPost(Base):
    __tablename__ = "user_post"
    __table_args__ = ()
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(sa.Integer, sa.ForeignKey(User.id))
    user: Mapped[User] = sa.orm.relationship(
        User,
        backref=sa.orm.backref("users"),
    )
    post_id: Mapped[int] = mapped_column(sa.Integer, sa.ForeignKey(Post.id))
    post: Mapped[Post] = sa.orm.relationship(
        Post,
        backref=sa.orm.backref("posts"),
    )


class PostComment(Base):
    __tablename__ = "post_comment"
    __table_args__ = (UniqueConstraint("post_id", "comment_id"),)
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    post_id: Mapped[int] = mapped_column(sa.Integer, sa.ForeignKey(Post.id))
    post: Mapped[Post] = sa.orm.relationship(
        Post,
        backref=sa.orm.backref("post"),
    )
    comment_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(Comment.id)
    )
    comment: Mapped[Comment] = sa.orm.relationship(
        Comment, backref=sa.orm.backref("comments")
    )


class UserTag(Base):
    __tablename__ = "user_tag"
    __table_args__ = (UniqueConstraint("user_id", "tag_id"),)
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(sa.Integer, sa.ForeignKey(User.id))
    user: Mapped[User] = sa.orm.relationship(
        User,
        backref=sa.orm.backref("user"),
    )
    tag_id: Mapped[int] = mapped_column(sa.Integer, sa.ForeignKey(Tag.id))
    tag: Mapped[Tag] = sa.orm.relationship(
        Tag,
        backref=sa.orm.backref("tags"),
    )


def setup(config: str) -> None:
    for doc in config_loader(config):
        database: str = doc.get("database", doc["index"])
        create_database(database)
        with pg_engine(database) as engine:
            Base.metadata.drop_all(engine)
            Base.metadata.create_all(engine)


@click.command()
@click.option(
    "--config",
    "-c",
    help="Schema config",
    type=click.Path(exists=True),
)
def main(config: str) -> None:
    config: str = get_config(config)
    teardown(config=config)
    setup(config)


if __name__ == "__main__":
    main()
