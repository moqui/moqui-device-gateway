<!--
This software is in the public domain under CC0 1.0 Universal plus a
Grant of Patent License.

To the extent possible under law, the author(s) have dedicated all
copyright and related and neighboring rights to this software to the
public domain worldwide. This software is distributed without any
warranty.

You should have received a copy of the CC0 Public Domain Dedication
along with this software (see the LICENSE.md file). If not, see
<http://creativecommons.org/publicdomain/zero/1.0/>.
-->
# Software Merge Completion Record

Consolidated 2026-11-09, working mr-moqui branches of moqui-device and
moqui-device-gateway. No production database migration or source-repository
deletion was performed. No commit or push is part of this record.

| Requested gate | Evidence |
| --- | --- |
| 1: Existing native drivers | 37 Rust tests passed; 33 Moqui gateway features passed with actual local Modbus TCP, OPC UA, MQTT 3/5 and pinned SFTP peers; HVAC outside this gateway scope was ignored |
| 2: Failure/recovery | 2 acceptance features passed: broker loss, immediate Postgres crash/recovery, recovery notification, bounded-queue overload, sustained collecting and restoration in a new native process |
| 5: Independent API | Separate Rust consumer executed a TCP read returning Decimal(38); normal dependency tree contains no Moqui framework crate |
| 6: Migration preparation | Dry-run/apply/idempotency/blocked endpoint/conflict-protected rollback tests passed; deployment procedure documented; old source remains available |

Supporting gates: common service/model/migration suite 33 passed, 9 optional
inference cases ignored; focused policy suite 10 passed; native debug build,
component Clippy with warnings denied, XML/schema/reference checks and whitespace
checks passed. Migration's 3 dedicated features are included in the common count.

An end-to-end OPC UA test exposed a timestamp conversion bug at the Rust/JS
boundary. The fix normalizes supported timestamp text and checks the fallback
date too. Its new regression passed. Test fixture issues (current BootOptions
fields, imported common service interfaces, stable broker ports across restart,
and deduplicated DB-independent outage control) were also corrected.


> Superseded on the same day for MQTT: durable subscriptions now acknowledge
> only stored messages and the queue applies backpressure instead of dropping.
> The overload run below dropped 1175 batches; the current acceptance drops
> none and persists 820 messages/s. See [validation](validation.md).

## Final Acceptance Metrics

```json
{
  "outageLogged": true,
  "recoveryNotified": true,
  "queueDrops": 1175,
  "persisted": 1000,
  "throughputMessages": 1000,
  "elapsedMillis": 20740,
  "persistedPerSecond": 48.216007714561236
}
```

The overload phase is separate from the throughput phase: 2200 batches were
sent while PostgreSQL was paused, 1025 were accepted and 1175 were explicitly
rejected by the bounded queue. Accepted batches drained after resume. The later
1000-message throughput phase persisted all 1000. These rates include debug-build,
transaction/callback and test-harness overhead on this machine.

There is no durable offline spool, no replay of rejected batches and no
exactly-once database delivery guarantee. SQL-backed alarms cannot persist while
their database is down; the tested recovery notice is sent after it returns.
This evidence does not establish industrial motion-control rates, cross-host
leader election, real PLC interoperability or credential/security completion.

## Artifacts And Reproduction

- `scripts/run-driver-tests.py`: driver log, OPC UA server log, exact SFTP file,
  independent host output and its dependency tree.
- `scripts/run-acceptance.py`: phase logs, Postgres crash recovery log and
  metrics.json under its printed private temporary artifact directory.
- `../moqui-device/scripts/test-services.sh`: common/model/migration regressions.
- `../moqui-device/scripts/audit-services.sh`: isolated policy regressions.

The final acceptance artifact directory was
`/tmp/moqui-gateway-acceptance-51j34egq`; the completed driver artifact directory
was `/tmp/moqui-driver-gates-o8fs588z`. Those temporary paths are provenance for
this run, not runtime configuration dependencies. The scripts recreate their
own artifacts on another machine. Test containers, servers and owned Postgres
processes were stopped; pre-existing user containers were left untouched.

See [validation](validation.md) for commands/limits and [migration](migration.md)
for installation preparation. The outstanding credential/security gate and golden
output corpus were not selected for this increment and are not marked complete.

