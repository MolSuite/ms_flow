"""A Ray cluster of this machine and others: the head here, each other machine joined over ssh.

The other machines need nothing but an environment that holds the same Ray, the same Python
and the functions that run (see `ms_env.app_recipe`), and to reach this machine and be reached
by it on the ports Ray opens. They join when the executor is made and leave when it is shut down.
"""
from __future__ import annotations

import logging
import shlex
import time

from ms_env import Host, HostError

from ms_flow.engine.executors import RayExecutor

log = logging.getLogger(__name__)


class RayOverHosts(RayExecutor):
    """`hosts` as `ssh` names them, `env` the path of the environment on each of them.

    No machine sees the disk of another: the files of an element travel through Ray.
    """

    def __init__(self, hosts, env: str, join_seconds: float = 90, **init):
        self._hosts, self._ray_there = [Host(address) for address in hosts], shlex.quote(f"{env}/bin/ray")
        # The address of this machine as the others reach it: where their ssh session comes from.
        # ponytail: asked to the first host, so all of them have to be on one network with this machine.
        here = self._hosts[0].run("echo ${SSH_CLIENT%% *}", timeout=60).decode().strip()
        super().__init__(None, shared=False, _node_ip_address=here, **init)
        address = self._ray.get_runtime_context().gcs_address
        try:
            for host in self._hosts:
                # ponytail: `ray stop` ends every Ray of the account on that machine, one left by a
                # session that died included. A host that runs a Ray of its own is not one to list here.
                host.run(f"{self._ray_there} stop >/dev/null 2>&1; {self._ray_there} start --address={address}",
                         timeout=join_seconds)
            limit = time.monotonic() + join_seconds
            while sum(node["Alive"] for node in self._ray.nodes()) <= len(self._hosts):
                if time.monotonic() > limit:
                    raise HostError(f"the machines that joined the Ray at {address} do not show in it: "
                                    "they have to reach this one, and this one them, on the ports Ray opens")
                time.sleep(0.5)
        except BaseException:
            self.shutdown()
            raise
        self.capacity = int(self._ray.cluster_resources().get("CPU", 1))

    def shutdown(self) -> None:
        for host in self._hosts:
            try:
                host.run(f"{self._ray_there} stop", timeout=60)
            except (HostError, OSError) as error:  # the cluster ends here either way
                log.warning("Ray was not stopped on %s: %s", host.address, error)
        super().shutdown()
