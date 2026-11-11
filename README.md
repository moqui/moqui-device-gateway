# moqui-device-gateway

Field communication drivers in Rust for [mr-moqui](https://github.com/moqui/mr-moqui)
and for any other Rust host: the role PLC4X and moqui-plc4j had on the JVM. Each
protocol wraps a native Rust library behind one facade, so that the caller does
not depend on the protocol, following the
[PLC4X concepts](https://plc4x.apache.org/plc4x/latest/users/getting-started/general-concepts.html):
a connection string `{driver code}:{transport code}://{transport config}?{options}`
and tag addresses `type:address`. Modern fieldbuses only: Modbus TCP, OPC UA and
MQTT now; EtherNet/IP, PROFINET, EtherCAT, BACnet and Modbus TLS to come. Files
are not a fieldbus: moqui-device moves them with mr-moqui's resources and SFTP.

| Branch | Content |
|---|---|
| `main` | The Quarkus/Camel standalone gateway |
| `old-mr-moqui` | The first standalone Rust rewrite, kept for reference |
| `mr-moqui` | This driver library |

The core knows connections, addresses and values, not Moqui. Under the `moqui`
feature the component has one service, as moqui-plc4j had `run#Plc4jRequest`:
`moqui.device.gateway.DeviceGatewayServices.run#DeviceGatewayRequest`, the
implementation of `moqui.device.DeviceServices.run#DeviceRequestInternal`
([moqui-device](https://github.com/moqui/moqui-device/tree/mr-moqui), which this
component depends on). A `DeviceRequest` names it in `runServiceName`; it turns
the request, its connection and items into a facade request, runs it on the
driver of the connection and hands the values to moqui-device's
`DeviceAcquisitionServices.store#Samples`, which owns the persistence.
`run#PlcLogRequest` subscribes to the log events of a moqui-plc PLC over MQTT.
`data/DeviceGatewayTestData.xml` and `DeviceGatewayDemoData.xml` set the
`runServiceName` of moqui-device's fieldbus requests.

## Structure

| Part | Content |
|---|---|
| `src/backends` | One backend per protocol, each behind its Cargo feature: `mqtt` (rumqttc), `opcua` (async-opcua), `modbus` (tokio-modbus) |
| `src/device_facade.rs` | `DeviceFacade`, the backend contract, and `SampleSink`, where continuous samples go |
| `src/client.rs` | `DeviceGatewayFacade`, the registry of connections and subscriptions, with one `execute` for every protocol |
| `src/model.rs`, `src/codec.rs`, `src/payload.rs` | Requests, items, values, scaling and the MQTT payload format |
| `src/tool.rs` | Feature `moqui`: `DeviceGatewayToolFactory` (tool `DeviceGateway`): one facade, its Tokio runtime, and the queue whose worker hands continuous samples to moqui-device |
| `src/binding.rs`, `src/notifier.rs` | Feature `moqui`: `run#DeviceGatewayRequest` and `run#PlcLogRequest`, the notice of samples that fail to store |

Without the `moqui` feature nothing of Moqui is linked:

```sh
cargo check --no-default-features --features mqtt,opcua,modbus
```

## Using the facade

`DeviceGatewayFacade::execute` takes a prepared request (operation, connection,
items with their address, physical type and scaling), input values, an absolute
deadline, output storage and, for subscriptions and polling, a `SampleSink`. It
needs no ExecutionContext. See [direct Rust access](examples/direct_modbus.rs)
and the independent host in `examples/rusty-host`, which reads a Modbus register
without a Moqui runtime or database.

A host creates one facade with a Tokio runtime. In MR-Moqui the
`DeviceGatewayToolFactory` does it (`MoquiConf.xml`, init-priority 30); its
`destroy` closes the connections and subscriptions, then the sample worker. A
running subscription asked to start again answers `unchanged`, so moqui-device's
`start#Subscriptions` can ask for every subscription that should run. A future
host, such as a PLC runtime on RuSTy, creates the same objects without the
ToolFactory and supplies its own `SampleSink`.

| Operation | MQTT 3.1.1 / 5 | OPC UA | Modbus TCP |
| --- | --- | --- | --- |
| Read | one message per item topic | typed values, per-node status and source timestamps | coils and typed registers; indexed bulk reads |
| Write | JSON values or one payload message; QoS 0/1/2 | typed values | coils and registers; grouped bulk writes |
| Subscribe | broker subscriptions with SUBACK and reconnect | monitored items | no |
| Poll | as a subscription | at the polling interval | at the polling interval |
| Unsubscribe | UNSUBSCRIBE, acknowledged | ends the subscription | ends the polling task |

Browse/discovery, structured OPC UA values and Modbus TLS are not implemented.
An unsupported request is an error; there is no fallback protocol.

All timeouts and polling intervals are milliseconds. One deadline covers session
acquisition and all items. A timed-out write is `uncertain` and is never resent
by the library. MQTT PUBACK confirms broker acceptance, not application of a
setpoint by the target device; QoS 0 only confirms local transmission.

## Connections

* Modbus: endpoint `modbus-tcp://host:502` (or driver `DcdModbusTCP` with
  `transportConfig=host:502`), options `unit-identifier=1` and optionally
  `byte-order=big-endian&word-order=big-endian`. Addresses use PLC4J-style
  zero-based offsets, e.g. `holding-register:2:UINT` or
  `holding-register:10:REAL`. No 40001 adjustment is inferred.
* MQTT: endpoint `mqtt://host:1883` or `mqtts://host:8883`; the topic is the
  item address. Options include `protocol-version=3|5`, `qos=0|1|2`, `retain`,
  `clean-start`, `session-expiry-seconds`, `automatic-reconnect`,
  `max-reconnect-delay-ms`, `receive-maximum` and `client-id`. Subscriptions
  have stable client IDs; finite calls use distinct IDs. TLS supports system
  roots or `ca-file`, optionally with `client-cert-file`/`client-key-file`.
* OPC UA: endpoint `opc.tcp://host:4840/path`; addresses are explicit NodeIds
  such as `ns=2;s=Temperature`. Options are `securityPolicy`, `securityMode`,
  `pkiDir`, `certificateFile`, `privateKeyFile`, `applicationUri`. Secure modes
  require client credentials and pretrusted server certificates; username
  authentication requires `SignAndEncrypt`.

Unknown protocol options are rejected rather than ignored.

## Durable MQTT ingestion

The broker, not the library, is the durable buffer. A subscription with a
persistent session (`clean-start=false`, the default for subscriptions) at QoS 1
or 2 acknowledges each message only when its `SampleSink` receipt completes: in
MR-Moqui, after the database commit of its samples, or after the store rejected
them for good. Until then the broker keeps it:

* Network loss between the broker and the host: the broker holds the messages
  published meanwhile and delivers them on reconnect.
* A sink that does not take more (a full queue, a database down): the
  subscription stops reading, and the broker keeps the rest. Nothing is dropped.
* Shutdown: what was stored is acknowledged, the rest stays unacknowledged, and
  the connection closes without UNSUBSCRIBE, so the session keeps collecting.
  Only an explicit unsubscribe removes the broker subscription.
* Crash: unacknowledged messages are delivered again.

Delivery is at-least-once. `receive-maximum` (default 256) bounds the
unacknowledged messages held. MQTT 5 announces it to the broker; with MQTT 3.1.1
the library pauses reading instead, and a pause longer than the keepalive makes
the broker close the connection and deliver the unacknowledged messages again.
Prefer MQTT 5 for durable subscriptions.

Polling and OPC UA samples have no broker behind them: a sink that is full
rejects them.

For ActiveMQ Artemis: `persistence-enabled` true, an `address-full-policy` of
`PAGE` for the MQTT addresses with paging sized for the longest outage,
`session-expiry-seconds` at least that outage (default 86400), and publishers at
QoS 1 or 2.

## Further protocols

PROFINET, EtherNet/IP, EtherCAT and the other `DeviceConnectionDriver` values
are not implemented yet. `DeviceFacade` is the extension point; choosing a
library requires checking its role (controller/device), licensing, supported
services and hardware requirements. Do not infer support from an enumeration or
a protocol name.

This library allocates, uses asynchronous networking, locks and queues. It is
not a hard real-time PLC runtime; see moqui-plc's `rust-plc` for the RuSTy
interfaces a PLC runtime would implement.

## Building and testing

Install this repository at `runtime/component/moqui-device-gateway` in mr-moqui;
the `moqui` feature uses the framework's sibling `moqui-core` and `moqui-lib`
crates. Rust 1.99 or later is required.

```sh
# From the mr-moqui root:
./moq build --debug -j 4
# From this directory: the driver tests, with their own Modbus peer and OPC UA server
cargo test
```

The service tests (`test/DeviceGateway.spec.js`) need moqui-math and moqui-device
installed. `scripts/run-driver-tests.py` starts local Modbus, OPC UA and MQTT
peers (Docker images `apache/activemq-artemis` and `atmoz/sftp:alpine`, the OPC UA
server of `examples/opcua_test_server.rs`, a Modbus peer) and runs the suites of
moqui-device and of this component with every feature;
`scripts/run-acceptance.py` adds broker loss, a database crash and stall, and a
restart (`acceptance/GatewayRecovery.spec.js`). Neither stops an existing broker
or database. See [validation](docs/validation.md) for
earlier results.
