# moqui-device-gateway

Field communication drivers in Rust for [mr-moqui](https://github.com/moqui/mr-moqui)
and for any other Rust host: the role PLC4X and moqui-plc4j had on the JVM. Each
protocol wraps a native Rust library behind one facade, so that the caller does
not depend on the protocol, following the
[PLC4X concepts](https://plc4x.apache.org/plc4x/latest/users/getting-started/general-concepts.html):
a connection string `{driver code}:{transport code}://{transport config}?{options}`
and tag addresses `type:address`. Modern fieldbuses only: Modbus TCP, OPC UA,
MQTT, EtherNet/IP and EtherCAT now; PROFINET, BACnet and Modbus TLS to come.
Files are not a fieldbus: moqui-device moves them with mr-moqui's resources and
SFTP.

| Protocol | Library | Feature | Driver (`DeviceConnection.driverEnumId`) |
|---|---|---|---|
| Modbus TCP | [tokio-modbus](https://crates.io/crates/tokio-modbus) 0.17 | `modbus` | `DcdModbusTCP`, `modbus-tcp://` |
| OPC UA | [async-opcua](https://crates.io/crates/async-opcua) 0.19 | `opcua` | `DcdOpcUa`, `opc.tcp://` |
| MQTT 3.1.1 / 5 | [rumqttc](https://crates.io/crates/rumqttc) 0.25 | `mqtt` | `DcdMqtt`, `mqtt://`, `mqtts://` |
| EtherNet/IP (Logix tags) | [rust-ethernet-ip](https://crates.io/crates/rust-ethernet-ip) 1.2 | `ethernetip` | `DcdLogixCIPEIP`, `DcdEthernetIP`, `logix://`, `eip://` |
| EtherCAT (MainDevice) | [ethercrab](https://crates.io/crates/ethercrab) 0.7 | `ethercat` | `DcdEtherCAT`, `ethercat://` |

All five are default features.

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
| `src/backends` | One backend per protocol, each behind its Cargo feature (see the table above) |
| `src/device_facade.rs` | `DeviceFacade`, the backend contract, and `SampleSink`, where continuous samples go |
| `src/client.rs` | `DeviceGatewayFacade`, the registry of connections and subscriptions, with one `execute` for every protocol |
| `src/model.rs`, `src/codec.rs`, `src/payload.rs` | Requests, items, values, scaling and the MQTT payload format |
| `src/tool.rs` | Feature `moqui`: `DeviceGatewayToolFactory` (tool `DeviceGateway`): one facade, its Tokio runtime, and the queue whose worker hands continuous samples to moqui-device |
| `src/binding.rs`, `src/notifier.rs` | Feature `moqui`: `run#DeviceGatewayRequest` and `run#PlcLogRequest`, the notice of samples that fail to store |

Without the `moqui` feature nothing of Moqui is linked:

```sh
cargo check --no-default-features --features mqtt,opcua,modbus,ethernetip,ethercat
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

The `requestTypeEnumId` of a `DeviceRequest` gives the operation:

* `DrtRead` reads once. With a `pollingInterval` it polls: the gateway reads
  again at every interval until a `DrtUnsubscribe`, and hands each read to the
  `SampleSink`. Polling is for protocols that cannot subscribe (Modbus), and the
  usual acquisition on OPC UA.
* `DrtStateChange`, `DrtCyclic` and `DrtEvent` are the subscription types of
  PLC4X; `DrtSubscribe` is the default of the protocol. The device sends the
  values: on a change, every `pollingInterval`, or as events.
* `DrtWrite` (and `DrtConfigWrite`) writes; `DrtUnsubscribe` ends the
  subscription or polling of its `parentRequestName`.

| Operation | MQTT 3.1.1 / 5 | OPC UA | Modbus TCP |
| --- | --- | --- | --- |
| `DrtRead` | error: a topic has no value to read | typed values, per-node status and source timestamps | coils and typed registers; indexed bulk reads |
| `DrtRead` with `pollingInterval` | error | reads at the interval | reads at the interval |
| `DrtWrite` | JSON values or one payload message; QoS 0/1/2 | typed values | coils and registers; grouped bulk writes |
| `DrtSubscribe`, `DrtEvent` | broker subscription with SUBACK and reconnect | `DrtSubscribe` as `DrtStateChange`; events are not implemented | error |
| `DrtStateChange` | as `DrtSubscribe` | monitored items, reported on change | error |
| `DrtCyclic` | error: a broker sends each message | monitored items sampled every `pollingInterval` | error |
| `DrtUnsubscribe` | UNSUBSCRIBE, acknowledged | ends the subscription or polling | ends the polling |

EtherNet/IP and EtherCAT are read, written and polled like Modbus TCP: a
subscription type on them is an error.

Browse/discovery, structured OPC UA values, OPC UA events and Modbus TLS are not
implemented. An unsupported request is an error; there is no fallback protocol.

Polling logs when a device stops and starts answering again, not every failed
cycle; each failure still reaches the store, which notifies a lasting one.

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
  authentication requires `SignAndEncrypt`. Reads and writes share a session per
  connection; each subscription has a session of its own, and a lost session
  ends the subscription until moqui-device's `start#Subscriptions` starts it
  again.
* EtherNet/IP: endpoint `logix://host` or `eip://host` (port 44818 by default),
  option `slot=0` for the backplane slot of the controller. CIP explicit
  messaging to the symbolic tags of Allen-Bradley Logix controllers
  (CompactLogix, ControlLogix), as the PLC4X `logix` driver: the address is the
  tag name as the controller knows it (`Motor_Speed`, `Line[3]`,
  `Recipe.Temperature`, `Program:Main.Counter`), and the item type gives the CIP
  type of a write (`DritDInt` is DINT, `DritReal` REAL, ...). Atomic types and
  STRING; a UDT is read and written member by member. Implicit (class 1) I/O
  connections are not implemented.
* EtherCAT: endpoint `ethercat://eth0`, the raw Ethernet interface of the
  segment; option `cycle-time-us` (default 2000). This host is the MainDevice:
  the first request on an interface discovers the SubDevices, brings them to OP
  and exchanges the process data every cycle in a thread of its own, until the
  host closes. An address is `input:SUBDEVICE:BYTE:TYPE` or
  `output:SUBDEVICE:BYTE:TYPE`, the SubDevice by its position on the segment (0
  is the first), the byte offset in its process data, little-endian; a BOOL is a
  bit, `input:1:0.3:BOOL`. A read takes the image of the last cycle and fails
  while a SubDevice is not in OP; a write changes the outputs and completes once
  two cycles have sent them. Only outputs are written. The mapping (PDOs) is the
  default of each SubDevice; SDO configuration and distributed clocks are not
  exposed. The first request on an interface fixes its cycle, and a request with
  another `cycle-time-us` is refused. A segment that stops on an error is
  started again by a request at least five seconds later. When the host closes,
  the SubDevices go to SAFE-OP and INIT: the outputs are no longer driven and
  each SubDevice applies its own safe state. A raw socket needs `CAP_NET_RAW` (or root) on Linux, and the cycle is
  as regular as the operating system makes an ordinary thread: no hard
  real-time.

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
rejects and logs them. The sample worker keeps and retries the batches it has
taken while the database does not answer.

A store failure and its recovery are Moqui notifications
(`DeviceInboundFailed`, `DeviceInboundRecovered`) and log entries. A notification
needs the database, so one about a database outage is sent after it recovers; it
is not an independent alarm channel.

For ActiveMQ Artemis: `persistence-enabled` true, an `address-full-policy` of
`PAGE` for the MQTT addresses with paging sized for the longest outage,
`session-expiry-seconds` at least that outage (default 86400), and publishers at
QoS 1 or 2.

## Values and writes

* Numeric conversions use Decimal; zero bounds and zero significant digits are
  meaningful, a zero scaling is rejected. A value beyond the `minValue` or
  `maxValue` of its ParameterDef is limited to that bound, on a read and before
  a write. An out-of-range write, or a fractional
  write to an integer physical type, fails instead of truncating.
* Modbus indexed bulk items share one response. A bulk write needs complete,
  unique indices for each array and sends one ordered PDU per bulk address. All
  values and PDU limits are checked before connecting.
* A write is never sent again by the library: a timed-out write is `uncertain`,
  retrying it is the caller's decision, and the receiver must be idempotent.
* An explicit MQTT unsubscribe sends an acknowledged UNSUBSCRIBE for every topic.
  With the broker unavailable, a persistent remote subscription can remain.
* MQTT TLS with a private CA or client certificates, and secured OPC UA, need
  tests on the installation; OPC UA rejects untrusted certificates and deprecated
  security policies.

## Further protocols

PROFINET, BACnet and the other `DeviceConnectionDriver` values are not
implemented yet. `DeviceFacade` is the extension point; choosing a
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
installed. `scripts/run-driver-tests.py` starts local peers (Docker images
`apache/activemq-artemis` and `atmoz/sftp:alpine`, which it does not pull, the OPC
UA server of `examples/opcua_test_server.rs`, a Modbus peer) and runs the suites
of moqui-device and of this component with every feature. With `CPPPO_PYTHON`
naming a Python that has [cpppo](https://pypi.org/project/cpppo/), it also
starts cpppo's Logix simulator and runs the EtherNet/IP feature; `cargo test`
runs the EtherNet/IP round trip when `GATEWAY_EIP_ENDPOINT` names one. EtherCAT
needs a segment on a raw interface, which no runner provides: its tests cover the
addresses, the process data image and an interface that cannot be opened, not
the cycle on real SubDevices.

`scripts/run-acceptance.py` (Linux, opt-in) runs `acceptance/GatewayRecovery.spec.js`:
broker stop and start, a cut of the gateway-broker link, a PostgreSQL crash, a
paused database under a 2200-message burst, a 1000-message persistence run, and
the restoration of subscriptions in a second host process, with messages
published while no gateway ran. Nothing may be lost or stored twice. It controls
only its own broker and database and keeps logs and metrics under the printed
temporary path. Physical devices, timing bounds and failover between hosts are
not covered by either runner.
