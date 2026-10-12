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


def _flag(name: str, asked: int, has: str, least: int = 0, times: int = 1) -> str:
    """The option of `ray start` that gives it `ms_flow.core.settings.models.share(asked, has, least)`,
    worked out by the shell of the machine, where `has` is the command that counts what it has."""
    if not asked:
        return ""  # Ray takes all it finds
    want = asked if asked > 0 else f"has + {asked}"
    return (f" --{name}=$(has=$({has}); n=$(({want})); n=$((n > has ? has : n)); "
            f"echo $(((n < {least} ? {least} : n) * {times})))")


class RayOverHosts(RayExecutor):
    """`hosts` as `ssh` names them, `env` the path of the environment on each of them: one for
    all, or one for each. `limits` gives for each the (CPUs, GPUs, GB of memory) that are for
    its tasks, as `HostConfig` counts them: without it a machine gives all it has.

    No machine sees the disk of another: the files of an element travel through Ray.
    """

    def __init__(self, hosts, env, join_seconds: float = 90, limits=None, **init):
        envs = [env] * len(hosts) if isinstance(env, str) else env
        self._hosts = [(Host(address), shlex.quote(f"{there}/bin/ray")) for address, there in zip(hosts, envs, strict=True)]
        gives = ["".join((_flag("num-cpus", cpus, "nproc", 1),
                          _flag("num-gpus", gpus, "nvidia-smi --list-gpus 2>/dev/null | wc -l"),
                          _flag("memory", memory_gb, "awk '/MemTotal/ {print int($2 / 1048576)}' /proc/meminfo", 1, 2 ** 30)))
                 for cpus, gpus, memory_gb in limits or [(0, 0, 0)] * len(hosts)]
        for host, _ray_there in self._hosts:
            # ponytail: Slurm is the one told apart, as on this machine (`job_api._login_node`).
            if host.run('command -v sbatch >/dev/null 2>&1 && [ -z "$SLURM_JOB_ID" ] && echo login || :', timeout=60).strip():
                raise HostError(f"{host.address} is the login node of a cluster, where nothing is to compute: "
                                "its queue is who runs jobs there")
        # The address of this machine as the others reach it: where their ssh session comes from.
        # ponytail: asked to the first host, so all of them have to be on one network with this machine.
        here = self._hosts[0][0].run("echo ${SSH_CLIENT%% *}", timeout=60).decode().strip()
        super().__init__(None, shared=False, _node_ip_address=here, **init)
        address = self._ray.get_runtime_context().gcs_address
        try:
            for (host, ray_there), given in zip(self._hosts, gives, strict=True):
                # ponytail: `ray stop` ends every Ray of the account on that machine, one left by a
                # session that died included. A host that runs a Ray of its own is not one to list here.
                host.run(f"{ray_there} stop >/dev/null 2>&1; {ray_there} start --address={address}{given}",
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
        for host, ray_there in self._hosts:
            try:
                host.run(f"{ray_there} stop", timeout=60)
            except (HostError, OSError) as error:  # the cluster ends here either way
                log.warning("Ray was not stopped on %s: %s", host.address, error)
        super().shutdown()
