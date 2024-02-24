"""PGSync Demo application."""

import aiohttp_cors
from aiohttp import web
from app import settings
from app.views import TypeAheadHandler, TypeAheadView


async def create_app():
    """Create the app."""
    app = web.Application()
    app.update(
        name="PGSync",
        settings=settings,
    )
    app.add_routes(
        [
            web.get("/typeahead", TypeAheadHandler),
            web.get("/", TypeAheadView),
        ]
    )
    cors = aiohttp_cors.setup(
        app,
        defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
            ),
        },
    )

    for route in list(app.router.routes()):
        cors.add(route)

    return app


if __name__ == "__main__":
    app = create_app()
