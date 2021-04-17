import json

import click
from schema import Comment, Post, PostComment, Tag, User, UserPost, UserTag
from sqlalchemy.orm import sessionmaker

from pgsync.base import pg_engine, subtransactions
from pgsync.helper import teardown
from pgsync.utils import get_config


@click.command()
@click.option(
    '--config',
    '-c',
    help='Schema config',
    type=click.Path(exists=True),
)
def main(config):

    config = get_config(config)
    teardown(drop_db=False, config=config)
    documents = json.load(open(config))
    engine = pg_engine(
        database=documents[0].get('database', documents[0]['index'])
    )
    Session = sessionmaker(bind=engine, autoflush=True)
    session = Session()

    # Bootstrap
    users = {
        'Carla Ferreira Cardoso': User(
            name='Carla Ferreira Cardoso', age=19, gender='female'
        ),
        'Uwe Fuerst': User(
            name='Uwe Fuerst', age=58, gender='male'
        ),
        'Otitodilinna Chigolum': User(
            name='Otitodilinna Chigolum', age=36, gender='male'
        ),
    }
    with subtransactions(session):
        session.add_all(users.values())

    posts = {
        'Post 1': Post(
            slug='post_1', title='This is the first post'
        ),
        'Post 2': Post(
            slug='post_2', title='This is the second post'
        ),
        'Post 3': Post(
            slug='post_3', title='This is the third post'
        ),
    }
    with subtransactions(session):
        session.add_all(posts.values())

    comments = {
        'Comment 1': Comment(
            title='Comment 1',
            content='This is a sample comment for comment 1',
        ),
        'Comment 2': Comment(
            title='Comment 2',
            content='This is a sample comment for comment 2',
        ),
        'Comment 3': Comment(
            title='Comment 3',
            content='This is a sample comment for comment 3',
        ),
        'Comment 4': Comment(
            title='Comment 4',
            content='This is a sample comment for comment 4',
        ),
        'Comment 5': Comment(
            title='Comment 5',
            content='This is a sample comment for comment 5',
        ),
        'Comment 6': Comment(
            title='Comment 6',
            content='This is a sample comment for comment 6',
        ),
    }
    with subtransactions(session):
        session.add_all(comments.values())

    tags = {
        'Economics': Tag(name='Economics'),
        'Career': Tag(name='Career'),
        'Political': Tag(name='Political'),
        'Fitness': Tag(name='Fitness'),
        'Entertainment': Tag(name='Entertainment'),
        'Education': Tag(name='Education'),
        'Technology': Tag(name='Technology'),
        'Health': Tag(name='Health'),
        'Fashion': Tag(name='Fashion'),
        'Design': Tag(name='Design'),
        'Photography': Tag(name='Photography'),
        'Lifestyle': Tag(name='Lifestyle'),
    }
    with subtransactions(session):
        session.add_all(tags.values())

    user_posts = [
        UserPost(
            user=users['Carla Ferreira Cardoso'],
            post=posts['Post 1'],
        ),
        UserPost(
            user=users['Uwe Fuerst'],
            post=posts['Post 2'],
        ),
        UserPost(
            user=users['Otitodilinna Chigolum'],
            post=posts['Post 3'],
        ),
    ]
    with subtransactions(session):
        session.add_all(user_posts)

    user_tags = [
        UserTag(
            user=users['Carla Ferreira Cardoso'],
            tag=tags['Economics'],
        ),
        UserTag(
            user=users['Carla Ferreira Cardoso'],
            tag=tags['Career'],
        ),
        UserTag(
            user=users['Carla Ferreira Cardoso'],
            tag=tags['Political'],
        ),
        UserTag(
            user=users['Carla Ferreira Cardoso'],
            tag=tags['Lifestyle'],
        ),
        UserTag(
            user=users['Carla Ferreira Cardoso'],
            tag=tags['Health'],
        ),
        UserTag(
            user=users['Uwe Fuerst'],
            tag=tags['Education'],
        ),
        UserTag(
            user=users['Uwe Fuerst'],
            tag=tags['Lifestyle'],
        ),
        UserTag(
            user=users['Otitodilinna Chigolum'],
            tag=tags['Fashion'],
        ),
    ]
    with subtransactions(session):
        session.add_all(user_tags)

    post_comments = [
        PostComment(
            post=posts['Post 1'],
            comment=comments['Comment 1'],
        ),
        PostComment(
            post=posts['Post 1'],
            comment=comments['Comment 2'],
        ),
        PostComment(
            post=posts['Post 2'],
            comment=comments['Comment 3'],
        ),
        PostComment(
            post=posts['Post 2'],
            comment=comments['Comment 4'],
        ),
        PostComment(
            post=posts['Post 3'],
            comment=comments['Comment 5'],
        ),
        PostComment(
            post=posts['Post 3'],
            comment=comments['Comment 6'],
        ),
    ]
    with subtransactions(session):
        session.add_all(post_comments)


if __name__ == '__main__':
    main()
