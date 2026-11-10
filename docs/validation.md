# Validation And Merge Gates

> 2026-11-10: the services moved to moqui-device. The service and acceptance
> runners are now moqui-device's `scripts/run-fieldbus-tests.py` and
> `scripts/run-acceptance.py`; this library keeps its Rust driver tests
> (`cargo test`). The results below were recorded before the move.

The local software gates cover the native API and the current MR services.
They do not certify a physical PLC, timing bounds, failover ownership across
hosts or private-CA/mTLS deployments. Lossless acquisition during an outage is
tested for durable MQTT subscriptions only; polling and OPC UA samples have no
broker to keep them.

## Reproduce

From this component, with Rust, Python 3, Docker and mosquitto_pub installed:

```bash
CARGO_BUILD_JOBS=4 cargo test --all-features -- --test-threads=1
python3 -B scripts/run-driver-tests.py
python3 -B scripts/run-acceptance.py
```

Build the native MR host first with `./moq build --debug -j 4` from mr-moqui.
The runners expect the locally available `apache/activemq-artemis:latest` and
`atmoz/sftp:alpine` images; they do not pull images implicitly. Driver tests use
free ports 12686 and 2223 for the OPC UA and SFTP peers. MQTT uses a private broker
on a free loopback port, including patching only the scratch seed records.

The driver runner starts its own OPC UA server, deterministic TCP Modbus peer
and pinned-host SFTP server. It verifies persisted read/write/poll/subscription
results and exact recipe file bytes, including replacement of an existing file.
It also builds and runs `examples/rusty-host` and checks its normal dependency
tree excludes moqui-core, moqui-entity, moqui-lib and moqui-host. The example is an
independent consumer of DeviceGatewayFacade/DeviceFacade, not a RuSTy runtime.

The acceptance runner is Linux-only (/proc process identity checks). It controls
only its own broker and the PostgreSQL cluster of its own scratch child process.
The loopback controller has a random capability token and deduplicates commands.
It tests broker stop/start, a cut of the gateway-broker link (a TCP proxy the
controller closes while the publisher keeps reaching the broker), immediate
PostgreSQL crash/recovery, recovery notices, a paused database under a
2200-message burst, a 1000-message persistence run, and subscription restoration
in a second native host process using the same database, with messages
published while no gateway ran. Resources are cleaned up; logs, metrics and scratch files are retained
under the printed temporary artifact path. These are opt-in acceptance tests,
never part of the ordinary regression run.

## Recorded Results

Working-tree verification after the durable MQTT ingestion, 2026-11-09:

| Gate | Result |
| --- | --- |
| Rust tests, all features | 38 passed, none failed; includes a durable subscription acknowledging only stored messages |
| Moqui gateway round-trips | 33 passed, none failed; one HVAC case ignored outside GW_EDGE_01 scope |
| Common services/model/migration | 32 passed, 1 failed, 9 ignored; the failure is the distributed `run#DeviceRequestGroup` refusal, which the updated framework now dispatches to a worker; it does not involve sample persistence |
| Focused service policy suite | 10 passed, none failed |
| Debug build and component Clippy | Passed |
| Recovery/load acceptance | 2 passed; nothing lost in any outage (metrics below) |

Acceptance metrics:

```json
{
  "networkCut": 300,
  "crashSamplesKept": true,
  "stalled": 2200,
  "queueDrops": 0,
  "persisted": 1000,
  "elapsedMillis": 1219,
  "persistedPerSecond": 820.3
}
```

* Link cut: 300 messages published while the gateway was disconnected were all
  stored after reconnection.
* PostgreSQL crash: the messages published while it was down were stored after
  its recovery (before, they were lost).
* PostgreSQL paused under a 2200-message burst: no queue rejection; all 2200
  stored after resume, none twice.
* Gateway stopped between the phases: the 200 messages published meanwhile were
  stored by the next process.
* 1000 messages persisted in 1.22 s (820 messages/s), against 48 messages/s with
  per-sample transactions, on the same debug build and machine. This is a local
  baseline, not a production capacity guarantee.

The end-to-end OPC UA gate found and fixed a real porting defect: timestamps
cross XML Actions as text, so common persistence must normalize valid epoch
millis/date text while rejecting malformed fallback dates. The regression checks
the stored observation time, not just a successful driver result.

## Delivery And Failure Semantics

The broker is the durable buffer: see Durable MQTT ingestion in the README. A
durable MQTT subscription (persistent session, QoS 1 or 2) acknowledges a message
only after the database commit of its samples, or after the store rejected them
for good. While the database does not answer the worker keeps the batch and
retries; the bounded queue (1024 batches) then fills and subscriptions stop
taking messages, which stay in the broker. Delivery is at-least-once: a logging
sample redelivered after a failure between commit and acknowledgement is a
duplicate when it carries the device time or a sequenceNum, and is stored again
otherwise.

Polling and OPC UA samples are rejected and logged when the queue is full; the
worker still keeps and retries the batches it has taken.

Store failures and recovery use Moqui notifications and logs. Notifications that
require the same unavailable database cannot persist during its outage; recovery
can be reported once the database is available. Do not infer an independent alarm
channel from a database-backed notification.

The transport reports an uncertain write without replaying it internally.
Moqui's common orchestration may retry that command with unchanged inputs and a
caller-controlled budget; the receiver must be idempotent. A process restart
restores subscriptions from the model, not command writes.

Concurrent native pollers have independent ownership/cancellation and shared
shutdown. This is not distributed leader election: deploy only one owner for a
gateway/device scope. Duplicate ParameterLog timestamps and conflicting writers
remain observable errors under the existing model constraints.

## Deployment Boundary

See [migration](migration.md) for preview/apply/rollback. No production database
or old source repository is changed by these runners. Physical-device validation,
credential-provider completion, secured deployments, golden output comparisons
and any new protocol adapters remain distinct gates.
