"""Tables, tasks and jobs used by the examples.

They live in their own module on purpose: a worker process imports the task again by
its `module:qualname`, so a function defined in the script you launch (`__main__`), a
lambda or a nested function will not do. The tables are SQLModel models: the project
creates them when it opens, as long as this module was imported before.
"""
from __future__ import annotations

from sqlmodel import Field, SQLModel

from ms_flow import job, table_sink, task
from ms_flow.query import QuerySpec, Rows


class Result(SQLModel, table=True):
    __tablename__ = "results"
    id: int | None = Field(default=None, primary_key=True)
    value: int
    double_value: int


class Molecule(SQLModel, table=True):
    __tablename__ = "molecules"
    id: int | None = Field(default=None, primary_key=True)
    name: str
    status: str


class Label(SQLModel, table=True):
    __tablename__ = "labels"
    id: int | None = Field(default=None, primary_key=True)
    molecule_id: int
    label: str


@task()
def double_value(value: int) -> dict:
    return {"value": value, "double_value": value * 2}


@job(task=double_value, output=table_sink("results", columns=("value", "double_value")))
def double_values(params, project):
    yield from params["values"]


@task()
def label_molecule(molecule: dict) -> dict:
    return {"molecule_id": molecule["id"], "label": molecule["name"].upper()}


@job(task=label_molecule, output=table_sink("labels", columns=("molecule_id", "label")))
def label_ready_molecules(params, project):
    return Rows(project, QuerySpec(table="molecules", fields=("id", "name"), filters={"status": "ready"}))
