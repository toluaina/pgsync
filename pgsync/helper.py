"""PGSync helpers."""

import logging
import os
import typing as t

import sqlalchemy as sa

from .base import database_exists, drop_database
from .sync import Sync
from .utils import config_loader, validate_config

logger = logging.getLogger(__name__)


def teardown(
    drop_db: bool = True,
    truncate_db: bool = True,
    delete_redis: bool = True,
    drop_index: bool = True,
    delete_checkpoint: bool = True,
    config: t.Optional[str] = None,
    schema_url: t.Optional[str] = None,
    s3_schema_url: t.Optional[str] = None,
    validate: bool = False,
) -> None:
    """
    Teardown helper.

    Args:
        drop_db (bool, optional): Whether to drop the database. Defaults to True.
        truncate_db (bool, optional): Whether to truncate the database. Defaults to True.
        delete_redis (bool, optional): Whether to delete Redis/Valkey. Defaults to True.
        drop_index (bool, optional): Whether to drop the index. Defaults to True.
        delete_checkpoint (bool, optional): Whether to delete the checkpoint. Defaults to True.
        config (Optional[str], optional): The configuration file path. Defaults to None.
        schema_url (Optional[str], optional): The schema URL. Defaults to None.
        s3_schema_url (Optional[str], optional): The S3 schema URL. Defaults to
        validate (bool, optional): Whether to validate the configuration. Defaults to False.
    """
    validate_config(
        config=config, schema_url=schema_url, s3_schema_url=s3_schema_url
    )

    for doc in config_loader(
        config=config, schema_url=schema_url, s3_schema_url=s3_schema_url
    ):
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
                os.unlink(sync.checkpoint_file)
            except (OSError, FileNotFoundError):
                pass
