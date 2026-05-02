import click
from schema import Department, Product, Shop
from sqlalchemy.orm import sessionmaker

from pgsync.base import pg_engine, subtransactions
from pgsync.helper import teardown
from pgsync.utils import config_loader, validate_config


@click.command()
@click.option(
    "--config",
    "-c",
    help="Schema config",
    type=click.Path(exists=True),
)
def main(config: str) -> None:
    validate_config(config)
    teardown(drop_db=False, config=config)
    doc: dict = next(config_loader(config))
    database: str = doc.get("database", doc["index"])
    with pg_engine(database) as engine:
        Session = sessionmaker(bind=engine, autoflush=True)
        session = Session()

        shops = [
            Shop(id=1, name="Downtown Mall", city="Springfield"),
            Shop(id=2, name="Riverside Plaza", city="Shelbyville"),
        ]
        with subtransactions(session):
            session.add_all(shops)

        departments = [
            Department(id=1, shop_id=1, name="Electronics", floor=1),
            Department(id=2, shop_id=1, name="Books", floor=2),
            Department(id=3, shop_id=2, name="Groceries", floor=1),
            Department(id=4, shop_id=2, name="Apparel", floor=2),
        ]
        with subtransactions(session):
            session.add_all(departments)

        products = [
            Product(id=1, department_id=1, name="Headphones", price=49.99),
            Product(id=2, department_id=1, name="Laptop", price=899.00),
            Product(id=3, department_id=2, name="Novel", price=14.50),
            Product(id=4, department_id=2, name="Cookbook", price=22.00),
            Product(id=5, department_id=3, name="Apples", price=3.20),
            Product(id=6, department_id=3, name="Bread", price=2.50),
            Product(id=7, department_id=4, name="T-Shirt", price=19.99),
            Product(id=8, department_id=4, name="Jeans", price=59.99),
        ]
        with subtransactions(session):
            session.add_all(products)


if __name__ == "__main__":
    main()
