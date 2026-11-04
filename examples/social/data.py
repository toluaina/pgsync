"""Sample data for the social example."""

import typing as t

from sqlalchemy.orm import Session

from .models import Comment, Post, PostComment, Tag, User, UserPost, UserTag


def seed(session: Session) -> None:
    users: t.List[User] = [
        User(name="Carla Ferreira Cardoso", age=19, gender="female"),
        User(name="Uwe Fuerst", age=58, gender="male"),
        User(name="Otitodilinna Chigolum", age=36, gender="male"),
    ]
    session.add_all(users)
    session.flush()
    posts: t.List[Post] = [
        Post(slug="post_1", title="This is the first post"),
        Post(slug="post_2", title="This is the second post"),
        Post(slug="post_3", title="This is the third post"),
    ]
    session.add_all(posts)
    session.flush()
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
    session.add_all(comments)
    session.flush()
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
    session.add_all(tags)
    session.flush()
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
    session.add_all(user_posts)
    session.flush()
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
    session.add_all(user_tags)
    session.flush()
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
    session.add_all(post_comments)
