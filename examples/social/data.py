import typing as t

import click
from schema import Comment, Post, PostComment, Tag, User, UserPost, UserTag
from sqlalchemy.orm import sessionmaker

from pgsync.base import pg_engine, subtransactions
from pgsync.helper import teardown
from pgsync.utils import config_loader, get_config


@click.command()
@click.option(
    "--config",
    "-c",
    help="Schema config",
    type=click.Path(exists=True),
)
def main(config: str) -> None:
    config: str = get_config(config)
    teardown(drop_db=False, config=config)
    doc: dict = next(config_loader(config))
    database: str = doc.get("database", doc["index"])
    with pg_engine(database) as engine:
        Session = sessionmaker(bind=engine, autoflush=True)
        session = Session()

        # Bootstrap
        users: t.List[User] = [
            User(name="Carla Ferreira Cardoso", age=19, gender="female"),
            User(name="Uwe Fuerst", age=58, gender="male"),
            User(name="Otitodilinna Chigolum", age=36, gender="male"),
        ]
        with subtransactions(session):
            session.add_all(users)

        posts: t.List[Post] = [
            Post(slug="post_1", title="This is the first post"),
            Post(slug="post_2", title="This is the second post"),
            Post(slug="post_3", title="This is the third post"),
        ]
        with subtransactions(session):
            session.add_all(posts)

        comments: t.List[Comment] = [
            Comment(
                title="Comment 1",
                content="This is a sample comment for comment 1",
            ),
            Comment(
                title="Comment 2",
                content="This is a sample comment for comment 2",
            ),
            Comment(
                title="Comment 3",
                content="This is a sample comment for comment 3",
            ),
            Comment(
                title="Comment 4",
                content="This is a sample comment for comment 4",
            ),
            Comment(
                title="Comment 5",
                content="This is a sample comment for comment 5",
            ),
            Comment(
                title="Comment 6",
                content="This is a sample comment for comment 6",
            ),
        ]
        with subtransactions(session):
            session.add_all(comments)

        tags: t.List[Tag] = [
            Tag(name="Economics"),
            Tag(name="Career"),
            Tag(name="Political"),
            Tag(name="Fitness"),
            Tag(name="Entertainment"),
            Tag(name="Education"),
            Tag(name="Technology"),
            Tag(name="Health"),
            Tag(name="Fashion"),
            Tag(name="Design"),
            Tag(name="Photography"),
            Tag(name="Lifestyle"),
        ]
        with subtransactions(session):
            session.add_all(tags)

        user_posts: t.List[UserPost] = [
            UserPost(
                user=users[0],
                post=posts[0],
            ),
            UserPost(
                user=users[1],
                post=posts[1],
            ),
            UserPost(
                user=users[2],
                post=posts[2],
            ),
        ]
        with subtransactions(session):
            session.add_all(user_posts)

        user_tags: t.List[UserTag] = [
            UserTag(
                user=users[0],
                tag=tags[0],
            ),
            UserTag(
                user=users[0],
                tag=tags[1],
            ),
            UserTag(
                user=users[0],
                tag=tags[2],
            ),
            UserTag(
                user=users[0],
                tag=tags[11],
            ),
            UserTag(
                user=users[0],
                tag=tags[7],
            ),
            UserTag(
                user=users[1],
                tag=tags[5],
            ),
            UserTag(
                user=users[1],
                tag=tags[11],
            ),
            UserTag(
                user=users[1],
                tag=tags[8],
            ),
        ]
        with subtransactions(session):
            session.add_all(user_tags)

        post_comments: t.List[PostComment] = [
            PostComment(
                post=posts[0],
                comment=comments[0],
            ),
            PostComment(
                post=posts[0],
                comment=comments[1],
            ),
            PostComment(
                post=posts[1],
                comment=comments[2],
            ),
            PostComment(
                post=posts[1],
                comment=comments[3],
            ),
            PostComment(
                post=posts[2],
                comment=comments[4],
            ),
            PostComment(
                post=posts[2],
                comment=comments[5],
            ),
        ]
        with subtransactions(session):
            session.add_all(post_comments)


if __name__ == "__main__":
    main()
