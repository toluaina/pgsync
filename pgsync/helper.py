"""PGSync helpers."""
import json
import logging
import os

import sqlalchemy as sa

from .base import drop_database
from .sync import Sync
from .utils import get_config

logger = logging.getLogger(__name__)


def teardown(
    drop_db=True,
    truncate_db=True,
    delete_redis=True,
    drop_index=True,
    delete_checkpoint=True,
    config=None,
):
    """Teardown helper."""
    config = get_config(config)

    for document in json.load(open(config)):

        sync = Sync(document, validate=False)
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
            sync.es.teardown(sync.index)
        if delete_redis:
            sync.redis._delete()
        if delete_checkpoint:
            try:
                os.unlink(sync._checkpoint_file)
            except OSError:
                pass
