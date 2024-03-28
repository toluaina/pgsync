"""PGSync helpers."""

import logging
import os
import typing as t

import sqlalchemy as sa

from .base import database_exists, drop_database
from .sync import Sync
from .utils import config_loader, get_config

logger = logging.getLogger(__name__)


def teardown(
    drop_db: bool = True,
    truncate_db: bool = True,
    delete_redis: bool = True,
    drop_index: bool = True,
    delete_checkpoint: bool = True,
    config: t.Optional[str] = None,
    validate: bool = False,
) -> None:
    """
    Teardown helper.

    Args:
        drop_db (bool, optional): Whether to drop the database. Defaults to True.
        truncate_db (bool, optional): Whether to truncate the database. Defaults to True.
        delete_redis (bool, optional): Whether to delete Redis. Defaults to True.
        drop_index (bool, optional): Whether to drop the index. Defaults to True.
        delete_checkpoint (bool, optional): Whether to delete the checkpoint. Defaults to True.
        config (Optional[str], optional): The configuration file path. Defaults to None.
        validate (bool, optional): Whether to validate the configuration. Defaults to False.
    """
    config: str = get_config(config)

    for doc in config_loader(config):
        if not database_exists(doc["database"]):
            logger.warning(f'Database {doc["database"]} does not exist')
            continue

        sync: Sync = Sync(doc, validate=validate)
        if truncate_db:
            try:
                sync.truncate_schemas()
                sync.engine.connect().close()
                sync.engine.dispose()
            except sa.exc.OperationalError as e:
                logger.warning(
                    f'Database "{sync.database}" does not exist: {e}'
                )
        if drop_db:
            drop_database(sync.database)
        if drop_index:
            sync.search_client.teardown(sync.index)
        if delete_redis:
            sync.redis.delete()
        if delete_checkpoint:
            try:
                os.unlink(sync._checkpoint_file)
            except (OSError, FileNotFoundError):
                pass
