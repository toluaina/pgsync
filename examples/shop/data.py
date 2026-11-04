"""Sample data for the shop example."""

from sqlalchemy.orm import Session

from .models import Department, Product, Shop


def seed(session: Session) -> None:
    shops = [
        Shop(id=1, name="Downtown Mall", city="Springfield"),
        Shop(id=2, name="Riverside Plaza", city="Shelbyville"),
    ]
    session.add_all(shops)
    session.flush()
    departments = [
        Department(id=1, shop_id=1, name="Electronics", floor=1),
        Department(id=2, shop_id=1, name="Books", floor=2),
        Department(id=3, shop_id=2, name="Groceries", floor=1),
        Department(id=4, shop_id=2, name="Apparel", floor=2),
    ]
    session.add_all(departments)
    session.flush()
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
    session.add_all(products)
