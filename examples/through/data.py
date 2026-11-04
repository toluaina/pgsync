"""Sample data for the through example."""

import typing as t

from sqlalchemy.orm import Session

from .models import Customer, CustomerGroup, Group


def seed(session: Session) -> None:
    customers: t.List[Customer] = [
        Customer(name="CustomerA"),
        Customer(name="CustomerB"),
        Customer(name="CustomerC"),
    ]
    session.add_all(customers)
    session.flush()
    groups: t.List[Group] = [
        Group(group_name="GroupA"),
        Group(group_name="GroupB"),
        Group(group_name="GroupC"),
    ]
    session.add_all(groups)
    session.flush()
    customers_groups: t.List[CustomerGroup] = [
        CustomerGroup(customer=customers[0], group=groups[0]),
        CustomerGroup(customer=customers[1], group=groups[1]),
        CustomerGroup(customer=customers[2], group=groups[2]),
    ]
    session.add_all(customers_groups)
