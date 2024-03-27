"""PGSync Demo server."""

from aiohttp import web
from app.main import create_app


def main():
    """PGSync Demo Webserver."""
    app = create_app()
    web.run_app(app)


if __name__ == "__main__":
    main()
