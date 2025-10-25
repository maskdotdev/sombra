#!/usr/bin/env python3
"""
Multi-Label Typed Node Example

This example demonstrates multi-label nodes with type-safe schema using the typed API.
Shows how to create nodes with multiple labels and proper type hints for IDE autocomplete.
"""

import os
import sys
from typing_extensions import TypedDict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))

from sombra.typed import SombraDB


class PersonProps(TypedDict):
    name: str
    age: int


class EmployeeProps(TypedDict):
    employee_id: str
    department: str
    salary: int


class ManagerProps(TypedDict):
    reports_to: int
    level: int


class NodeSchema(TypedDict):
    Person: PersonProps
    Employee: EmployeeProps
    Manager: ManagerProps


class ManagesEdgeProps(TypedDict):
    pass


class EdgeSchema(TypedDict):
    MANAGES: ManagesEdgeProps


class OrgGraphSchema(TypedDict):
    nodes: NodeSchema
    edges: EdgeSchema


def main():
    db_path = "multi_label_typed.db"

    if os.path.exists(db_path):
        os.unlink(db_path)

    print("Multi-Label Typed Node Example\n")

    db: SombraDB[OrgGraphSchema] = SombraDB(db_path)

    print("1. Creating CEO (Person only)...")
    ceo = db.add_node(
        "Person",
        {
            "name": "Sarah Chen",
            "age": 45,
        },
    )
    print(f"Created CEO: {ceo}\n")

    print("2. Creating VP (Person + Employee + Manager)...")
    vp = db.add_node(
        ["Person", "Employee", "Manager"],
        {
            "name": "John Smith",
            "age": 38,
            "employee_id": "E001",
            "department": "Engineering",
            "salary": 150000,
            "reports_to": ceo,
            "level": 1,
        },
    )
    print(f"Created VP: {vp}\n")

    print("3. Creating Engineer (Person + Employee)...")
    engineer = db.add_node(
        ["Person", "Employee"],
        {
            "name": "Alice Johnson",
            "age": 28,
            "employee_id": "E123",
            "department": "Engineering",
            "salary": 120000,
        },
    )
    print(f"Created Engineer: {engineer}\n")

    db.add_edge(vp, engineer, "MANAGES", {})

    print("4. Querying by labels...")
    all_people = db.get_nodes_by_label("Person")
    all_employees = db.get_nodes_by_label("Employee")
    all_managers = db.get_nodes_by_label("Manager")

    print(f"People: {len(all_people)} (nodes: {', '.join(map(str, all_people))})")
    print(
        f"Employees: {len(all_employees)} (nodes: {', '.join(map(str, all_employees))})"
    )
    print(
        f"Managers: {len(all_managers)} (nodes: {', '.join(map(str, all_managers))})\n"
    )

    print("5. Inspecting nodes...")
    vp_node = db.get_node(vp)
    print(
        "VP Node:",
        {
            "id": vp_node.id,
            "labels": vp_node.labels,
            "properties": vp_node.properties,
        },
    )

    engineer_node = db.get_node(engineer)
    print(
        "\nEngineer Node:",
        {
            "id": engineer_node.id,
            "labels": engineer_node.labels,
            "properties": engineer_node.properties,
        },
    )

    print("\n✅ Multi-label typed example complete!")

    os.unlink(db_path)


if __name__ == "__main__":
    main()
