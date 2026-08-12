# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Cold-resume lifecycle workload: resume/suspend random pre-seeded actors.

Unlike AteAPIUser (ate_api.py), which creates one cache-warm actor per user,
ColdLifecycleUser picks a UNIFORMLY RANDOM actor from a large pre-seeded
population (e.g. 10M actors seeded by storebench with
--seed-actor-prefix/--seed-template-*) on every cycle, so at XL volumes the
actor reads pay real cold-page costs. Designed for the fake-atelet setup
(docs/dev/fake-lifecycle-testing.md).

Two users may pick the same actor: the loser's Resume/Suspend fails with
Aborted (lease/CAS conflict) or FailedPrecondition (wrong state). Those are
counted as failures by locust but the user just moves on — with millions of
actors and hundreds of users, collisions are rare.

Cycle: ResumeActor(random i) -> SuspendActor(same i).
"""

import random

from common.grpc_setup import init_grpc_gevent

init_grpc_gevent()

import grpc
from locust import User, events, task
from locust.argument_parser import LocustArgumentParser
from common import ateapi_pb2
from common import ateapi_pb2_grpc
from common.ateapi_channel import ateapi_channel
from common.grpc_tracing import traced_grpc
from common.metrics import init_metrics, update_user_count
from common.trace import init_tracing
from common.wait_time import init_wait_time, dynamic_wait_time
import logging

logger = logging.getLogger(__name__)

init_tracing()
init_metrics()
init_wait_time()


@events.init_command_line_parser.add_listener
def _add_options(parser: LocustArgumentParser) -> None:
    parser.add_argument("--cold-actor-count", type=int, default=10_000_000,
                        env_var="LOCUST_COLD_ACTOR_COUNT",
                        help="Size of the pre-seeded resumable actor population")
    parser.add_argument("--cold-actor-prefix", type=str, default="cactor-",
                        env_var="LOCUST_COLD_ACTOR_PREFIX",
                        help="Actor name prefix used at seeding time")
    parser.add_argument("--cold-atespace-count", type=int, default=100_000,
                        env_var="LOCUST_COLD_ATESPACE_COUNT",
                        help="Atespace count the population was seeded across")
    parser.add_argument("--cold-atespace-prefix", type=str, default="sbench-",
                        env_var="LOCUST_COLD_ATESPACE_PREFIX",
                        help="Atespace name prefix used at seeding time")


class ColdLifecycleUser(User):
    """Each iteration: resume a uniformly random seeded actor, then suspend it."""

    wait_time = dynamic_wait_time
    host = "api.ate-system.svc.cluster.local:443"

    def on_start(self) -> None:
        update_user_count(1, self.__class__.__name__)
        self.channel = ateapi_channel(self.host)
        self.stub = ateapi_pb2_grpc.ControlStub(self.channel)
        opts = self.environment.parsed_options
        self.count = opts.cold_actor_count
        self.actor_prefix = opts.cold_actor_prefix
        self.atespace_count = opts.cold_atespace_count
        self.atespace_prefix = opts.cold_atespace_prefix

    def on_stop(self) -> None:
        update_user_count(-1, self.__class__.__name__)
        self.channel.close()

    def _ref(self, i: int) -> ateapi_pb2.ObjectRef:
        # Must mirror storebench's dataset naming exactly:
        #   atespace = <atespace_prefix>%06d (i % atespace_count)
        #   actor    = <actor_prefix>%09d
        return ateapi_pb2.ObjectRef(
            atespace=f"{self.atespace_prefix}{i % self.atespace_count:06d}",
            name=f"{self.actor_prefix}{i:09d}",
        )

    @task
    def cold_cycle(self) -> None:
        ref = self._ref(random.randrange(self.count))

        try:
            with traced_grpc("ResumeActor", self.__class__.__name__) as metadata:
                _, metadata.call = self.stub.ResumeActor.with_call(
                    ateapi_pb2.ResumeActorRequest(actor=ref), metadata=metadata
                )
        except grpc.RpcError:
            # Lease/CAS collision with another user, or unexpected state:
            # recorded by traced_grpc as a failure; skip the suspend.
            return

        try:
            with traced_grpc("SuspendActor", self.__class__.__name__) as metadata:
                _, metadata.call = self.stub.SuspendActor.with_call(
                    ateapi_pb2.SuspendActorRequest(actor=ref), metadata=metadata
                )
        except grpc.RpcError:
            pass
