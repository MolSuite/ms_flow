# API reference

The reference below is generated from the Python sources. Names beginning
with an underscore are implementation details.

## Workspace

::: ms_flow.main.Workspace
    options:
      inherited_members: true
      show_if_no_docstring: true

## Tasks and jobs

::: ms_flow.engine
    options:
      members:
        - task
        - job
        - report
        - launched
        - content_key
        - writer
        - JobSnapshot
        - TaskSnapshot

## Sinks

::: ms_flow.sinks

## Queries

::: ms_flow.query

## The engine

::: ms_flow.engine
    options:
      members:
        - Engine
        - ProcessExecutor
        - ThreadExecutor
        - RayExecutor
        - Flow

## Hosts and executors

::: ms_flow.core.settings.models.HostConfig

::: ms_flow.core.settings.models.ExecutorConfig
