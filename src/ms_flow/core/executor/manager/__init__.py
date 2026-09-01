"""ExecutorManager package.

The manager is composed with focused runtime services (job commands, status writes,
dispatch, feeding, backend control) while keeping the public import path unchanged:
``from ms_flow.core.executor.manager import ExecutorManager``.
"""

from ms_flow.core.executor.manager._manager import (
    CallbackResultHandler,
    DataContractError,
    ExecutorManager,
    JobFeed,
)

__all__ = ["ExecutorManager", "DataContractError", "JobFeed", "CallbackResultHandler"]
