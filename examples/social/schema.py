import json

import click
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import UniqueConstraint

from pgsync.base import create_database, pg_engine
from pgsync.helper import teardown
from pgsync.utils import get_config

Base = declarative_base()


class User(Base):
    __tablename__ = 'user'
    __table_args__ = (
        UniqueConstraint('name'),
    )
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String, nullable=False)
    age = sa.Column(sa.Integer, nullable=True)
    gender = sa.Column(sa.String, nullable=True)


class Post(Base):
    __tablename__ = 'post'
    __table_args__ = ()
    id = sa.Column(sa.Integer, primary_key=True)
    title = sa.Column(sa.String, nullable=False)
    slug = sa.Column(sa.String, nullable=True)


class Comment(Base):
    __tablename__ = 'comment'
    __table_args__ = ()
    id = sa.Column(sa.Integer, primary_key=True)
    title = sa.Column(sa.String, nullable=True)
    content = sa.Column(sa.String, nullable=True)


class Tag(Base):
    __tablename__ = 'tag'
    __table_args__ = (
        UniqueConstraint('name'),
    )
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String, nullable=False)


class UserPost(Base):
    __tablename__ = 'user_post'
    __table_args__ = ()
    id = sa.Column(sa.Integer, primary_key=True)
    user_id = sa.Column(
        sa.Integer, sa.ForeignKey(User.id)
    )
    user = sa.orm.relationship(
        User,
        backref=sa.orm.backref('users'),
    )
    post_id = sa.Column(
        sa.Integer, sa.ForeignKey(Post.id)
    )
    post = sa.orm.relationship(
        Post,
        backref=sa.orm.backref('posts'),
    )


class PostComment(Base):
    __tablename__ = 'post_comment'
    __table_args__ = (
        UniqueConstraint('post_id', 'comment_id'),
    )
    id = sa.Column(sa.Integer, primary_key=True)
    post_id = sa.Column(
        sa.Integer, sa.ForeignKey(Post.id)
    )
    post = sa.orm.relationship(
        Post,
        backref=sa.orm.backref('post'),
    )
    comment_id = sa.Column(
        sa.Integer, sa.ForeignKey(Comment.id)
    )
    comment = sa.orm.relationship(
        Comment,
        backref=sa.orm.backref('comments')
    )


class UserTag(Base):
    __tablename__ = 'user_tag'
    __table_args__ = (
        UniqueConstraint('user_id', 'tag_id'),
    )
    id = sa.Column(sa.Integer, primary_key=True)
    user_id = sa.Column(
        sa.Integer, sa.ForeignKey(User.id)
    )
    user = sa.orm.relationship(
        User,
        backref=sa.orm.backref('user'),
    )
    tag_id = sa.Column(
        sa.Integer, sa.ForeignKey(Tag.id)
    )
    tag = sa.orm.relationship(
        Tag,
        backref=sa.orm.backref('tags'),
    )


def setup(config=None):
    for document in json.load(open(config)):
        database = document.get('database', document['index'])
        create_database(database)
        engine = pg_engine(database=database)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)


@click.command()
@click.option(
    '--config',
    '-c',
    help='Schema config',
    type=click.Path(exists=True),
)
def main(config):

    config = get_config(config)
    teardown(config=config)
    setup(config)


if __name__ == '__main__':
    main()
