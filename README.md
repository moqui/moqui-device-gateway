# moqui-device-gateway

`moqui-device-gateway` is a **model-first and data-driven industrial edge gateway** for devices modeled with the `moqui-device` component.

Unlike flow-diagram-first tools, where the developer manually builds behavior with ad hoc drag-and-drop flows, this gateway starts from a **shared, relational, industrial device model**. Devices, PLCs, parameters, requests, subscriptions, transport endpoints, gateway ownership, recipes, device configurations, and runtime activation rules are declared as structured data in the Moqui database. Apache Camel routes are then generated and activated as runtime projections of that model.

The result is a different class of gateway:

- **model-first**: the `moqui-device`, a complete Device Lifecycle Management (DLM) / Asset Management data model for industrial/IoT devices, serves as the single source of truth, providing a stronger and more auditable foundation for data consistency, traceability, operational governance, and cybersecurity review. Using the database as the operational declaration layer gives stronger governance than hand-built runtime diagrams.
- **data-driven**: seed data and database rows determine what the gateway does;
- **AI-friendly**: AI agents can inspect, validate, generate, and load structured device data instead of trying to reason over arbitrary visual diagrams;
- **integration-rich**: Apache Camel provides a large ecosystem of components and Enterprise Integration Patterns (EIP), so the same model can drive MQTT, OPC UA, file transfer, database persistence, and future protocols;
- **cloud/edge ready**: Quarkus provides fast startup, Java 21 virtual-thread support, container deployment, and optional GraalVM native compilation.

This is the key differentiation: the gateway does not ask developers to draw fragile runtime flows by hand. It lets engineers and AI agents work against a validated industrial data model, then uses Camel to execute that model safely at the edge.

The Moqui database is the source of truth. Camel routes are runtime projections of the model.

---

## Model-first before data lake

A `moqui-device` gateway does not start from the idea that industrial data should first be collected into an unstructured data lake and interpreted later. It starts from the device model.
Devices, PLCs, parameters, recipes, requests, gateway ownership, telemetry, and route activation are linked to explicit model entities from the beginning.

A data lake can still be useful as a downstream analytical or long-term storage layer for raw signals, images, logs, and AI training datasets. However, it is no longer the semantic core of the architecture. The semantic source of truth is the `moqui-device` model; the lake, if present, is a derived storage target.

---

## Why this is different from diagram-first gateways

Many industrial gateway and low-code tools start from visual flows. The developer connects nodes, wires protocol blocks, adds transformations, and progressively builds a runtime diagram. This can be useful for quick prototypes, but the diagram often becomes the source of truth.

`moqui-device-gateway` follows a different approach. The source of truth is not a diagram and not an unstructured data lake. The source of truth is the `moqui-device` model stored in the Moqui database.

| Aspect                  | Diagram-first gateway                                                  | Data-lake-first approach                              | `moqui-device-gateway`                                                                                                    |
| ----------------------- | ---------------------------------------------------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Primary source of truth | Visual flows manually created by developers                            | Raw data collected first, interpreted later           | Structured industrial device model                                                                                        |
| Device knowledge        | Spread across diagrams, node settings, scripts, and naming conventions | Often reconstructed after ingestion                   | Explicit `Device`, `PhysicalDevice`, `DeviceGroup`, `ParameterDef`, `DeviceRequest`, `DeviceConfig`, and related entities |
| Gateway behavior        | Encoded in flow diagrams                                               | Usually outside the lake, in separate ingestion jobs  | Declared as data and executed as Camel routes                                                                             |
| Semantics               | Implicit in the diagram                                                | Often missing or added later                          | Present before data collection starts                                                                                     |
| AI assistance           | Must interpret visual flows and ad hoc configuration                   | Must infer meaning from raw tables, topics, and files | Can inspect and generate structured model data                                                                            |
| Change control          | Flow diff and review can be difficult                                  | Raw data does not describe operational intent         | Database records and seed XML are reviewable, versionable, and auditable                                                  |
| Runtime execution       | Tool-specific flow runtime                                             | External pipelines and jobs                           | Apache Camel routes with Enterprise Integration Patterns                                                                  |
| Long-term analytics     | Usually added separately                                               | Central architectural focus                           | Optional downstream sink fed by a model-aware system                                                                      |

The practical consequence is important: the gateway does not merely move data from PLCs to a database or broker. It executes a modeled industrial system. A telemetry value, a recipe parameter, a PLC request, or a subscription route is not an anonymous data point; it is connected to a device, a parameter definition, a configuration, a request, and a gateway responsibility.

This is what makes the component model-driven and AI-friendly. AI agents do not have to reverse-engineer the meaning of arbitrary diagrams or unstructured lake records. They can work against explicit model entities, validate completeness, generate seed data, review route intent, and help activate Camel routes from a controlled operational model.

---

## AI-ready engineering workflow

Because the gateway is driven by model data, AI agents can participate safely in the engineering workflow without becoming the source of truth. The agent does not need to reverse-engineer a visual flow or edit opaque runtime wiring. It can operate on explicit entities, relationships, enumerations, seed XML, SQL checks, and tests.

This makes the gateway suitable for AI-assisted industrial engineering workflows such as:

- reviewing whether a gateway is correctly associated with its PLCs through `DeviceGroup` and `DeviceGroupMember`;
- checking whether `DeviceRequest` and `DeviceRequestItem` rows are complete before a route is activated;
- generating or reviewing seed data for devices, parameters, requests, and subscriptions;
- detecting missing gateway identity, missing PLC membership, invalid request scope, or incomplete transport configuration;
- producing repeatable activation and test procedures from the model;
- supporting safer deployment reviews before Camel routes run at the edge.

The important point is not that AI replaces engineering validation. The point is that a structured industrial model gives AI agents a precise and reviewable control surface. With diagram-first tools, the agent usually has to reason over informal visual wiring. With `moqui-device-gateway`, the agent reasons over database-backed model entities and executable tests.

---

## What this component does

`moqui-device-gateway` runs outside the main Moqui web application, close to PLCs, brokers, OPC UA servers, and OT network resources.

It can:

- execute `DeviceRequest` rows defined in the `moqui-device` model;
- publish MQTT messages from `Parameter` values;
- subscribe to MQTT topics and store inbound values;
- read, write, or subscribe to OPC UA nodes;
- ingest PLC diagnostic logs;
- export device configuration, recipe, and batch-management data;
- transfer device content when configured (CNC G-Code file, binary code, upgrade, docs, etc.);
- expose REST endpoints to trigger configured requests;
- restore startup subscriptions from the Moqui database.

It does **not**:

- replace Moqui;
- define the canonical device model by itself;
- use local JSON files as the subscription source of truth;
- require hand-built flow diagrams as the primary configuration mechanism;
- decide which PLC belongs to which gateway by hard-coded Java logic.

---

## Runtime architecture

```text
Moqui database
  └─ moqui-device model
      ├─ Device / PhysicalDevice
      ├─ DeviceGroup / DeviceGroupMember
      ├─ DeviceRequest / DeviceRequestItem
      ├─ ParameterDef / Parameter / ParameterLog
      ├─ DeviceConfig (atomic recipe/configuration definition)
      ├─ DeviceRuleSet / DeviceRule (batch-management, phased application, validation, export)
      └─ DeviceConnection
            ↓
moqui-device-gateway
  └─ Quarkus + Apache Camel runtime routes
            ↓
MQTT / OPC UA / PLC / PostgreSQL / file transfer
```

The gateway does not hard-code PLCs, parameters, MQTT topics, OPC UA nodes, or device recipes. It reads model rows and creates runtime behavior from them.

The main rule is:

```text
moqui-device model rows = persistent declaration
Camel routes = runtime execution
```

---

## moqui-device model used by the gateway

| Entity | Purpose in the gateway |
|---|---|
| `Device` | Logical identity of a gateway, PLC, controller, IIoT controller, remote IO, drive/inverter, soft starter, servodrive or other modeled device. |
| `PhysicalDevice` | Physical representation of a gateway, PLC, controller, remote IO, drive/inverter, soft starter, servodrive, broker adapter, or other real device. |
| `DeviceGroup` | Logical group that associates an edge gateway with the PLCs/devices it is responsible for. |
| `DeviceGroupMember` | Membership and role of each device inside a `DeviceGroup`, including `DgmpEdgeGateway` for the gateway process. |
| `DeviceRequest` | Declarative request to execute: read, write, subscribe, unsubscribe, export, transfer, and similar operations. |
| `DeviceRequestItem` | Parameters, topics, node IDs, or other item-level targets belonging to a request. |
| `ParameterDef` | Definition of a machine, process, telemetry, command, recipe, or configuration parameter. |
| `Parameter` | Current value or state of a parameter. |
| `ParameterLog` | Historical/inbound values written by telemetry routes. |
| `DeviceConfig` | Atomic machine-side recipe/configuration definition: target values, limits, modes, trajectories, or settings for one compatible device type. |
| `DeviceRuleSet` | Root-scoped operational plan for one `Device`: production cycle, commissioning sequence, validation plan, or batch-style procedure. |
| `DeviceRule` | Concrete phase/operation that applies, checks, asserts, suggests, or validates one `DeviceConfig` on one target `Device`. |
| `DeviceConnection` | Optional transport connection details, especially useful for OPC UA and direct device links on fieldbus protocols. |
| `DeviceConfig.approximatedFunctionId` | Optional FK to a `moqui-math` trajectory (`ApproximatedFunction`). When set, the config export generates `Trajectory.*` recipe entries in addition to the regular `Parameter` rows. |

In this model, a recipe is not a separate hand-coded gateway flow. A recipe is a machine-side `DeviceConfig`: a structured configuration of parameters and rules that can be stored, reviewed, versioned, exported, and applied through the same model used for devices, telemetry, and requests. This aligns the gateway with established recipe and batch-management concepts, where configuration, execution, and traceability must stay aligned.

The gateway assumes the real production schema and seed lifecycle are managed by Moqui and the `moqui-device` component. The SQL files under `src/test/resources` are local test fixtures and examples only.

---

## Gateway to PLC association

The gateway itself is modeled as a `Device` + `PhysicalDevice`.

A PLC is also modeled as a `Device` + `PhysicalDevice`.

The association between a gateway and the PLCs it serves is modeled through `DeviceGroup` and `DeviceGroupMember`.

Example:

```text
DeviceGroup:
  DG_LINE_01_EDGE

DeviceGroupMember:
  DG_LINE_01_EDGE / GW_EDGE_01  / DgmpEdgeGateway
  DG_LINE_01_EDGE / PLC_LINE_01 / DgmpProcessPLC
  DG_LINE_01_EDGE / PLC_LINE_02 / DgmpProcessPLC
```

The important enumeration is:

```xml
<moqui.basic.Enumeration
    enumId="DgmpEdgeGateway"
    description="Edge Gateway"
    enumTypeId="DeviceGroupMemberPurpose"/>
```

Meaning:

- `DgmpEdgeGateway` identifies the gateway process responsible for a group;
- `DgmpProcessPLC`, `DgmpSafetyPLC`, `DgmpController`, `DgmpRemoteIO`, and similar values identify target PLC/controller/remote-IO members;
- `DeviceRequest.deviceId` remains the target device, usually the PLC;
- `DeviceRequest.brokerUri` remains the broker or transport endpoint;
- `DeviceRequest.query` and/or `DeviceRequestItem.query` remain the topic, node, or target address.

`DgmpEdgeGateway` is not the MQTT broker. It is the edge process that executes the request. The broker is still represented by the request transport fields, such as `brokerUri`, or by `DeviceConnection` where appropriate.

---

## Startup subscription discovery

At startup or after a failure recovery, the gateway discovers active subscriptions from the Moqui database.

Startup discovery follows this chain:

```text
GATEWAY_DEVICE_ID
  -> Device + PhysicalDevice validation
  -> DeviceGroupMember with purposeEnumId = DgmpEdgeGateway
  -> target PLC/controller/remote-IO members in the same DeviceGroup
  -> active DeviceRequest rows routed through DrrMoquiDeviceGateway
  -> dynamic Camel routes
```

In practical terms, the gateway starts only from its logical identity, `GATEWAY_DEVICE_ID`. From that identity it finds the PLCs/controllers it is responsible for, then loads the active `DeviceRequest` rows that belong to those target devices. The result is a set of Camel routes created from the model at runtime.

Manual REST execution follows the same ownership rule: a gateway can execute only requests belonging to devices inside its own `DeviceGroup` scope.

---

## Runtime configuration

The most important runtime settings are:

| Variable / property | Required | Description |
|---|---:|---|
| `GATEWAY_DEVICE_ID` / `gateway.device.id` | Yes | `Device.deviceId` of this gateway, with a matching `PhysicalDevice` row, for example `GW_EDGE_01`. |
| `QUARKUS_DATASOURCE_JDBC_URL` | Yes | JDBC URL of the Moqui database. |
| `QUARKUS_DATASOURCE_USERNAME` | Yes | Database user. |
| `QUARKUS_DATASOURCE_PASSWORD` | Yes | Database password. |
| `QUARKUS_DATASOURCE_LOG_JDBC_URL` | Optional but recommended | JDBC URL of the telemetry/log datasource used for `PARAMETER_LOG`, `DEVICE_LOG`, and related observability tables. In production this is typically a TimescaleDB-backed PostgreSQL database separate from the transactional `moqui` DB. |
| `GATEWAY_API_AUTH_TOKEN` | Required when API auth is enabled | REST API token. Do not use the default token in production. |
| `QUARKUS_HTTP_PORT` | Optional | HTTP port, default `8081`. |

Example:

```bash
export GATEWAY_DEVICE_ID=GW_EDGE_01
export QUARKUS_DATASOURCE_JDBC_URL=jdbc:postgresql://127.0.0.1:5432/moqui
export QUARKUS_DATASOURCE_USERNAME=moqui
export QUARKUS_DATASOURCE_PASSWORD=moqui
export QUARKUS_DATASOURCE_LOG_JDBC_URL=jdbc:postgresql://127.0.0.1:5432/moqui-log
export GATEWAY_API_AUTH_TOKEN='replace-with-a-real-secret'
```

`GATEWAY_DEVICE_ID` must exist as `Device` + `PhysicalDevice` and must belong to at least one `DeviceGroup` as `DgmpEdgeGateway`.

---

## Local test seed data

The SQL files under `src/test/resources` define a self-contained virtual industrial setup used by the local developer workflow and integration tests. All IDs use a `__SUFFIX__` placeholder; the Docker Compose file (`docker/postgres-compose.yml`) expands it to `01`, giving concrete IDs such as `GW_EDGE_01`, `VIRTUAL_PLC_01`, `DRV_EDGE_01`, and `DG_EDGE_01`. The canonical scenario is mirrored in Moqui XML seed data under `moqui-framework/runtime/component/moqui-device/data/DeviceTestData.xml`.

The transactional model stays in the `moqui` database, while time-series and telemetry ingestion are intended for the `moqui-log` database. For lightweight integration checks the gateway test profile may still point both datasources to the same local PostgreSQL database, but the runtime configuration remains split and independently configurable.

### Devices and group

The seed models one edge gateway process and one virtual PLC, associated through a `DeviceGroup`:

```text
DeviceGroup:  DG_EDGE_01
  GW_EDGE_01      → DgmpEdgeGateway  (the gateway process)
  VIRTUAL_PLC_01  → DgmpProcessPLC   (the simulated PLC target)
```

Both `GW_EDGE_01` and `VIRTUAL_PLC_01` have a matching `Device` + `PhysicalDevice` row.

### Parameters

Five operational parameters are seeded on `VIRTUAL_PLC_01`:

| `ParameterDef` | `Parameter` | Type | Initial value | Meaning |
|---|---|---|---|---|
| `VPL_PD_REFERENCE_01` | `VPL_PARAM_REFERENCE_01` | Decimal | `300.00` | Speed or reference setpoint written to the PLC |
| `VPL_PD_FEEDBACK_01` | `VPL_PARAM_FEEDBACK_01` | Decimal | `300.03` | Speed or measurement feedback from the PLC |
| `VPL_PD_MAIN_CONTROL_WORD_01` | `VPL_PARAM_MAIN_CONTROL_WORD_01` | Text | `START` | Control word sent to the PLC |
| `VPL_PD_MAIN_STATUS_WORD_01` | `VPL_PARAM_MAIN_STATUS_WORD_01` | Text | `READY` | Status word read from the PLC |
| `VPL_PD_FAULT_01` | `VPL_PARAM_FAULT_01` | Indicator | `N` | Fault flag (`N` = no fault, `Y` = fault active) |

### MQTT requests (`device-gateway-seed.sql`)

| `DeviceRequest` | Type | What it does |
|---|---|---|
| `VPL_MQTT_PUBLISH_REQ_01` | `DrtWrite` | Publishes `VPL_PARAM_REFERENCE_01` and `VPL_PARAM_MAIN_CONTROL_WORD_01` to MQTT topics under `mqtt-write-device-request/virtual-plc/` |
| `VPL_MQTT_SUB_REQ_01` | `DrtCyclic` | Subscribes to `mqtt-subscribe-device-request/virtual-plc/feedback` and `.../fault`; inbound values update `VPL_PARAM_FEEDBACK_01` and `VPL_PARAM_FAULT_01` |
| `VPL_MQTT_UNSUB_REQ_01` | `DrtUnsubscribe` | Tears down the subscription created by `VPL_MQTT_SUB_REQ_01` |

### OPC UA connection and requests (`device-gateway-opcua-seed.sql`)

A `DeviceConnection` (`VPL_OPCUA_CONN_01`) defines the OPC UA TCP endpoint for `VIRTUAL_PLC_01`. Its transport address and node IDs are resolved from placeholders at database initialization time.

| `DeviceRequest` | Type | What it does |
|---|---|---|
| `VPL_OPCUA_READ_REQ_01` | `DrtCyclic` | Creates an OPC UA subscription (100 ms sampling) on the feedback and fault nodes; values update `VPL_PARAM_FEEDBACK_01` and `VPL_PARAM_FAULT_01` |
| `VPL_OPCUA_ONESHOT_REQ_01` | `DrtRead` | Performs a single one-shot read of the feedback node and stores the result in `VPL_PARAM_FEEDBACK_01` |
| `VPL_OPCUA_WRITE_REQ_01` | `DrtWrite` | Writes the current value of `VPL_PARAM_REFERENCE_01` to the writable OPC UA node |
| `VPL_OPCUA_UNSUB_REQ_01` | `DrtUnsubscribe` | Tears down the subscription created by `VPL_OPCUA_READ_REQ_01` |

The OPC UA node IDs exposed by the embedded Milo test server are:

| Placeholder | Node ID | Parameter |
|---|---|---|
| `__OPCUA_FEEDBACK_NODE__` | `ns=2;s=virtual_plc_feedback` | `VPL_PARAM_FEEDBACK_01` (Double, read-only) |
| `__OPCUA_FAULT_NODE__` | `ns=2;s=virtual_plc_fault` | `VPL_PARAM_FAULT_01` (String, read-only) |
| `__OPCUA_REFERENCE_WRITE_NODE__` | `ns=2;s=virtual_plc_reference_write` | `VPL_PARAM_REFERENCE_01` (Double, read-write) |

### Recipe configuration (`device-gateway-seed.sql`)

A minimal recipe is seeded to exercise the device configuration export scenario:

| Entity | ID | Detail |
|---|---|---|
| `DeviceConfig` | `VPL_CONFIG_1_01` | Recipe definition (`DctApplyConfig`) for `VIRTUAL_PLC_01` |
| `Parameter` (recipe) | `VPL_CFG_PARAM_REFERENCE_01` | Decimal `250.0` — recipe reference setpoint |
| `Parameter` (recipe) | `VPL_CFG_PARAM_STATE_01` | Text `AUTO` — recipe process mode |
| `DeviceRuleSet` | `VPL_RULESET_1_01` | Binds the recipe to a process context |
| `DeviceRule` | `VPL_RULE_1_01` | `DrtApplyConfig` rule linking `VPL_CONFIG_1_01` to `VIRTUAL_PLC_01` |

Scenario E (device configuration export) uses `VPL_RULESET_1_01` as input to generate a configuration export from `VPL_CONFIG_1_01`.

### Trajectory export seed data (`device-gateway-seed.sql`)

A minimal motion trajectory is seeded to exercise the trajectory section of the device configuration export:

| Entity | ID | Detail |
|---|---|---|
| `DeviceConfig` | `TRJ_CONFIG_1_01` | Config of type `DtRobot` linked to trajectory `TRJ_TEST_01_01` via `approximatedFunctionId` |
| `DeviceRuleSet` | `TRJ_RULESET_1_01` | Rule set used as input to the export endpoint |
| `DeviceRule` | `TRJ_RULE_1_01` | `DrtApplyConfig` rule binding `TRJ_CONFIG_1_01` to `VIRTUAL_PLC_01` |
| `ApproximatedFunctionSample` | `AFS_PT1_01`, `AFS_PT2_01` | Two trajectory waypoints, `sequenceNum` 1 and 2 |
| `TrajectoryPoint` | (same PKs) | `pt[0]` normal blending, `pt[1]` is a break point (`isBreakPoint = Y`) |
| `ParametricPathPoint` | `AFS_PT1_01` only | `tolerance = 10.0` → exported as `blendingMode` for `pt[0]`; `pt[1]` defaults to `0.0` |
| `Vector` / `VectorComponent` | `VEC_PT1_01`, `VEC_PT2_01` | 3 Cartesian dimensions per point (X=100/150, Y=200/250, Z=300/350) |

Scenario E with `TRJ_RULESET_1_01` generates a recipe file containing `Trajectory.*` fields (see below).

---

## Quick start for developers

### 1. Build

```bash
./gradlew clean build -x test
```

Tests are excluded from the `build` task so that the artifact can be built independently from test execution. This is the standard pattern for Quarkus/Gradle projects.

### 2. Run fast tests

```bash
./gradlew test
```

Fast tests run without the full Moqui runtime and without external MQTT/OPC UA infrastructure. They are excluded from the `build` task above and must be run explicitly.

> **Note:** Camel logs one expected `ERROR` line ("Failed delivery... Inbound payload must be a Map or List of Map rows") during the `malformedInboundPayloadIsDiscardedWithoutStoppingConsumerRoute` test. This is intentional — the test verifies that a malformed payload is rejected without stopping the consumer route.

### 3. Start local PostgreSQL

```bash
docker compose -f docker/postgres-compose.yml -p moqui-gateway up -d
```

The local PostgreSQL Compose file initializes automatically on first startup. It loads:

```text
src/test/resources/db/init.sql
src/test/resources/device-gateway-seed.sql
src/test/resources/device-gateway-opcua-seed.sql
```

The seed templates are expanded with the environment variables defined in `docker/postgres-compose.yml`.

If you want to re-run initialization from scratch:

```bash
docker compose -f docker/postgres-compose.yml -p moqui-gateway down -v
docker compose -f docker/postgres-compose.yml -p moqui-gateway up -d
```

Wait until PostgreSQL is healthy:

```bash
docker ps --filter name=moqui-gateway-database
```

Verify that the local fixture data exists:

```bash
PGPASSWORD=moqui psql -h 127.0.0.1 -p 5432 -U moqui -d moqui \
  -c "select device_id from device order by device_id;"
```

Password for the local compose file:

```text
moqui
```

This creates sample IDs such as:

```text
GW_EDGE_01
VIRTUAL_PLC_01
DG_EDGE_01
VPL_MQTT_PUBLISH_REQ_01
VPL_MQTT_SUB_REQ_01
```

### 4. Start ActiveMQ Artemis for MQTT tests

For local MQTT tests, use the ActiveMQ Artemis broker provided by the standard Moqui Docker setup.

From the repository layout where `moqui-device-gateway` is next to `moqui-framework`, start Artemis with:

```bash
docker compose -f ../moqui-framework/docker/activemq-compose.yml -p moqui-gateway up -d
```

The local seed examples expect an MQTT broker compatible with:

```text
paho-mqtt5:?brokerUrl=tcp://localhost:1883&userName=artemis&password=artemis
```

This is the standard local developer mode:

- PostgreSQL via `docker/postgres-compose.yml`
- ActiveMQ Artemis via `../moqui-framework/docker/activemq-compose.yml`
- `moqui-device-gateway` running directly on the host JVM with Quarkus/Gradle

In this standard mode, `DeviceRequest.brokerUri` should use `tcp://localhost:1883`.

Container gateway mode is a separate option. If the gateway itself runs with `docker/device-gateway-compose.yml`, then `DeviceRequest.brokerUri` must use a broker host name reachable from inside the gateway container. That host name depends on the service name and network used by the Moqui framework ActiveMQ Compose file, so it should not be guessed or hard-coded blindly.

Any MQTT broker can be used if the corresponding `DeviceRequest.brokerUri` values are updated, but ActiveMQ Artemis is the reference broker for this component.

### 5. Start the gateway without Docker

For local developer startup, use the `local` profile:

```bash
GATEWAY_DEVICE_ID=GW_EDGE_01 \
java -Dquarkus.profile=local -jar build/quarkus-app/quarkus-run.jar
```

Equivalent Quarkus dev-mode startup:

```bash
GATEWAY_DEVICE_ID=GW_EDGE_01 \
./gradlew quarkusDev -Dquarkus.profile=local
```

Default endpoint:

```text
http://localhost:8081
```

If you want to start with the default profile instead, use a real token:

```bash
GATEWAY_DEVICE_ID=GW_EDGE_01 \
GATEWAY_API_AUTH_TOKEN='replace-with-a-real-secret' \
java -jar build/quarkus-app/quarkus-run.jar
```

### 6. Check readiness

```bash
curl http://localhost:8081/q/health/ready
```

Readiness should be `UP` only when the process, database, Camel context, and gateway model identity are valid.

### 7. Trigger a request

```bash
curl -X POST \
  http://localhost:8081/api/device-request/run/VPL_MQTT_PUBLISH_REQ_01 \
  -H 'X-API-Key: change-me'
```

---

## Running scenarios

### Scenario A — MQTT write / publish

Use this when a `DeviceRequest` represents a write/publish operation.

Prerequisites:

- the gateway exists as `Device` + `PhysicalDevice`;
- the target PLC exists as `Device` + `PhysicalDevice`;
- both are in the same `DeviceGroup`;
- the gateway member has `purposeEnumId = DgmpEdgeGateway`;
- the PLC member has a PLC/controller purpose such as `DgmpProcessPLC`;
- a `DeviceRequest` exists with `requestTypeEnumId = DrtWrite`;
- request items exist in `DeviceRequestItem`;
- current values exist in `Parameter`.

For an MQTT live-parameter request, each `DeviceRequestItem` uses:

- `parameterId` to reference the existing, device-bound `Parameter` whose current value is published;
- `requestItemName` as the application-specific JSON key consumed by the generated PLC mapper;
- `query` as the item topic, when it overrides the request-level topic.

The gateway publishes both the canonical fields and the mapper key. For example, an item with
`parameterId = P_TEMP_SETPOINT`, `requestItemName = tempSetpoint`, and numeric value `22.5` produces:

```json
{"parameterId":"P_TEMP_SETPOINT","numericValue":22.5,"tempSetpoint":22.5}
```

The names `parameterId`, `numericValue`, `symbolicValue`, and `parameterEnumId` are reserved and
cannot be used as `requestItemName` values.

These examples use `mosquitto_sub` only as an MQTT client CLI tool. The broker used by the standard local setup is ActiveMQ Artemis.

> **Order matters:** start `mosquitto_sub` before triggering the request, otherwise the messages are published before the subscription is open and nothing is received.

Open the subscription in one terminal:

```bash
mosquitto_sub -h 127.0.0.1 -p 1883 \
  -u artemis -P artemis \
  -t 'mqtt-write-device-request/#'
```

Then, in a second terminal, trigger the request:

```bash
curl -X POST \
  http://localhost:8081/api/device-request/run/VPL_MQTT_PUBLISH_REQ_01 \
  -H 'X-API-Key: change-me'
```

### Scenario B — MQTT subscription / inbound parameter update

For persistent startup subscriptions, do not edit a local JSON file.

Create or seed an active `DeviceRequest` in the Moqui database and associate the target PLC with the gateway through `DeviceGroupMember`.

Required model rows:

```text
Device + PhysicalDevice: gateway
Device + PhysicalDevice: PLC
DeviceGroup: gateway/PLC scope
DeviceGroupMember: gateway with DgmpEdgeGateway
DeviceGroupMember: PLC with DgmpProcessPLC or similar
DeviceRequest: subscription request routed through DrrMoquiDeviceGateway
DeviceRequestItem: subscription items/topics/parameters where required
```

Start the gateway. It will restore eligible subscription routes from the database at startup. The log will confirm which subscriptions were restored:

```text
Restored DB-defined subscription for DeviceRequest VPL_MQTT_SUB_REQ_01 on startup.
Gateway GW_EDGE_01 restored 2 DB-defined subscription route(s) from 1 DeviceRequest row(s).
```

These examples use `mosquitto_pub` only as an MQTT client CLI tool. The broker used by the standard local setup is ActiveMQ Artemis.

Publish a test inbound message:

```bash
mosquitto_pub -h 127.0.0.1 -p 1883 \
  -u artemis -P artemis \
  -t 'mqtt-subscribe-device-request/virtual-plc/feedback' \
  -m '{"parameterId":"VPL_PARAM_FEEDBACK_01","numericValue":12.3,"purposeEnumId":"DrpLogging"}'
```

Verify the value was logged:

```bash
PGPASSWORD=moqui psql -h 127.0.0.1 -p 5432 -U moqui -d moqui \
  -c "SELECT parameter_id, numeric_value, observed_date FROM PARAMETER_LOG ORDER BY observed_date DESC LIMIT 5;"
```

Expected result: a new row with `numeric_value = 12.3` and the current timestamp.

Also check the current parameter value:

```bash
PGPASSWORD=moqui psql -h 127.0.0.1 -p 5432 -U moqui -d moqui \
  -c "SELECT parameter_id, numeric_value, last_updated_stamp FROM PARAMETER WHERE PARAMETER_ID = 'VPL_PARAM_FEEDBACK_01';"
```

**Note on `purposeEnumId`:** the payload field `purposeEnumId` controls what the gateway writes:

| `purposeEnumId` | `PARAMETER` updated | `PARAMETER_LOG` written |
|---|:---:|:---:|
| `DrpLogging` | No | Yes |
| any other value (or omitted) | Yes | Yes |

In this example `DrpLogging` is used, so the current value in `PARAMETER` is **not** updated — only the historical log row in `PARAMETER_LOG` is written. This is expected behavior.

### Scenario C — PLC log ingestion

> **Prerequisite:** PLC log ingestion is only available on PLCs that have the [moqui-plc](https://github.com/moqui-industrial/moqui-plc) framework installed. The CODESYS LoggerFacade MQTT appender provided by `moqui-plc` is what generates the structured log payloads described below.

PLC log ingestion routes inbound diagnostic records from a PLC into the database. The transport is MQTT; the `plc-log-consumer` route listens on the topic configured by `plc.log.consume.uri`. In the `local` developer profile the topic is `moqui-plc`.

Typical flow:

```text
PLC (moqui-plc / CODESYS LoggerFacade)
  -> MQTT topic: moqui-plc
  -> plc-log-consumer route
  -> PARAMETER_LOG  (entries with a non-empty source)
  -> DEVICE_LOG     (entries with an empty source)
```

#### Payload format

The PLC publishes a **numbered-object** JSON batch. Each top-level key is a sequential string index (`"1"`, `"2"`, ...); each value is one log entry:

```json
{
  "1": {
    "logEventDate": "DT#2026-05-25-10:00:00",
    "loggerName":   "hvac_controller",
    "source":       "tempSensor",
    "type":         1,
    "repeatCount":  1,
    "numericValue": 21.5
  },
  "2": {
    "logEventDate": "DT#2026-05-25-10:00:01",
    "loggerName":   "hvac_controller",
    "source":       "",
    "type":         0,
    "repeatCount":  1,
    "message":      "Cycle started."
  }
}
```

| Field | Description |
|---|---|
| `logEventDate` | IEC 61131-3 `DT#YYYY-MM-DD-HH:MM:SS` timestamp |
| `loggerName` | Logger identifier; used as `deviceId` in `DEVICE_LOG` and as the `parameterId` prefix in `PARAMETER_LOG` |
| `source` | Data source or sensor name within the logger; empty string means a device-level message with no associated parameter |
| `type` | `1` = numeric value, `0` = text / symbolic message |
| `numericValue` | (type 1 only) the numeric measurement |
| `message` | (type 0 only) the text message |
| `repeatCount` | Number of consecutive identical events |

#### Routing rules

| `source` field | Route destination | Key used |
|---|---|---|
| Non-empty (e.g. `"tempSensor"`) | `PARAMETER_LOG` | `parameterId = {loggerName}.{source}` |
| Empty (`""`) | `DEVICE_LOG` | `deviceId = loggerName` |

The route auto-creates `ParameterDef` and `Parameter` rows for each `loggerName.source` pair before writing to `PARAMETER_LOG`.

#### Test with mosquitto_pub

The `local` profile starts the `plc-log-consumer` route automatically on `moqui-plc`.

Publish a numeric entry:

```bash
mosquitto_pub -h 127.0.0.1 -p 1883 \
  -u artemis -P artemis \
  -t 'moqui-plc' \
  -m '{"1":{"logEventDate":"DT#2026-05-25-10:00:00","loggerName":"hvac_controller","source":"tempSensor","type":1,"repeatCount":1,"numericValue":21.5}}'
```

Verify `PARAMETER_LOG`:

```bash
PGPASSWORD=moqui psql -h 127.0.0.1 -p 5432 -U moqui -d moqui \
  -c "SELECT parameter_id, numeric_value, observed_date FROM PARAMETER_LOG WHERE parameter_id LIKE 'hvac_controller.%' ORDER BY observed_date DESC LIMIT 5;"
```

Publish a device-level (no-source) entry:

```bash
mosquitto_pub -h 127.0.0.1 -p 1883 \
  -u artemis -P artemis \
  -t 'moqui-plc' \
  -m '{"1":{"logEventDate":"DT#2026-05-25-10:00:01","loggerName":"hvac_controller","source":"","type":0,"repeatCount":1,"message":"Framework started."}}'
```

Verify `DEVICE_LOG`:

```bash
PGPASSWORD=moqui psql -h 127.0.0.1 -p 5432 -U moqui -d moqui \
  -c "SELECT device_id, payload, observed_date FROM DEVICE_LOG WHERE device_id = 'hvac_controller' ORDER BY observed_date DESC LIMIT 5;"
```

### Scenario D — OPC UA

#### Integration test — embedded Milo server (no external OPC UA server needed)

The project includes `MiloTestServer`, an embedded Eclipse Milo OPC UA server that starts in-process during `OpcUaGatewayIntegrationTest`. No external OPC UA server or simulator is required to run these tests. The test resolves a free TCP port at runtime and fills in the seed SQL placeholders automatically.

The embedded server exposes three nodes:

| Node identifier | Type | Access | Purpose |
|---|---|---|---|
| `virtual_plc_feedback` | Double | Read-only | one-shot read and subscribe tests (inbound → `PARAMETER`) |
| `virtual_plc_fault` | String | Read-only | subscribe test (inbound → `PARAMETER`) |
| `virtual_plc_reference_write` | Double | Read-write | write test (gateway writes `PARAMETER` value to node) |

To run the OPC UA integration tests:

```bash
# Infrastructure: PostgreSQL + Artemis must be running (same as Scenario C)
./gradlew integrationTest --tests '*OpcUaGatewayIntegrationTest' -Dquarkus.profile=integration
```

The tests verify:

1. **one-shot read** (`DrtRead`) — gateway reads the current value of an OPC UA node once and stores it into `PARAMETER`;
2. **subscribe** (`DrtCyclic`) — gateway creates an OPC UA subscription; Milo server pushes new values; gateway writes them into `PARAMETER`;
3. **write** (`DrtWrite`) — gateway reads a `PARAMETER` current value and writes it to the writable OPC UA node.

#### Manual test or production setup with a real OPC UA server

Use this path when `DeviceRequest` / `DeviceRequestItem` rows reference a real OPC UA endpoint and node IDs.

The sample seed is at:

```text
src/test/resources/device-gateway-opcua-seed.sql
```

It contains placeholders that must be replaced before use:

| Placeholder | Replace with |
|---|---|
| `__SUFFIX__` | your ID suffix, e.g. `01` |
| `__OPCUA_TRANSPORT_CONFIG__` | OPC UA server address and path, e.g. `192.168.1.100:4840/moqui` |
| `__OPCUA_FEEDBACK_NODE__` | OPC UA node ID for the feedback parameter, e.g. `ns=2;s=channel1.device1.feedback` |
| `__OPCUA_FAULT_NODE__` | OPC UA node ID for the fault parameter |
| `__OPCUA_REFERENCE_WRITE_NODE__` | OPC UA node ID for the write target |

Typical setup once the seed is adapted:

1. seed the database with the filled SQL;
2. start the OPC UA server;
3. start the gateway (`local` or production profile);
4. trigger a subscribe request via the REST API;
5. verify that `PARAMETER` rows are updated as the OPC UA server publishes new values;
6. trigger a write request and verify the value on the OPC UA server side.

### Scenario E — Device configuration / recipe export

Configuration export is used when Moqui stores a device configuration, recipe, or batch-management definition and the gateway must export it for an external PLC/device workflow. In this model, a recipe is a structured machine-side `DeviceConfig`, not a separate visual flow.

The export produces one `.txtrecipe` file per priority group, named `{ruleSetName}_p{priority:02d}.txt`. Each line follows the CODESYS `.txtrecipe` format:

```text
DeviceName.ParameterName:=value
```

Example REST call (standard recipe):

```bash
curl -X POST \
  http://localhost:8081/api/device-config/export \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: change-me' \
  -d '{
    "deviceRuleSetId":"VPL_RULESET_1_01",
    "deviceId":"VIRTUAL_PLC_01",
    "deviceRuleId":null
  }'
```

#### Trajectory export

When `DeviceConfig.approximatedFunctionId` is set, the export appends `Trajectory.*` fields to the recipe file in addition to the regular `Parameter` rows. The trajectory data is read from the `moqui-math` tables (`ApproximatedFunctionSample`, `TrajectoryPoint`, `ParametricPathPoint`, `Vector`, `VectorComponent`) via a single SQL query with CTE-based UNION ALL sections.

The generated recipe fields map directly to the CODESYS `Trajectory` struct declared in `moqui-plc`:

| Recipe field | Source | Notes |
|---|---|---|
| `Trajectory.trajectoryId` | `DeviceConfig.approximatedFunctionId` | Trajectory identity |
| `Trajectory.pointCount` | COUNT of trajectory points | Total waypoints |
| `Trajectory.points[n].sequenceNum` | `ApproximatedFunctionSample.sequenceNum` | 1-based sequence |
| `Trajectory.points[n].timeOffsetMs` | `TrajectoryPoint.pointTimeOffsetMillis` | Time offset in ms |
| `Trajectory.points[n].pos.v[d]` | `VectorComponent.realValue` | Position per dimension index |
| `Trajectory.points[n].isBreakPoint` | `TrajectoryPoint.isBreakPoint` | `TRUE` / `FALSE` |
| `Trajectory.points[n].hasBlendingOverride` | `TrajectoryPoint.blendingEnumId IS NOT NULL` | `TRUE` / `FALSE` |
| `Trajectory.points[n].blendingMode` | `ParametricPathPoint.tolerance` | Defaults to `0.0` when no row exists |
| `Trajectory.points[n].bufferMode` | `blendingEnumId` (PLCopen `MC_BUFFER_MODE` int) | `PpcmPlcAborting`=0, `PpcmPlcBuffered`=1, `PpcmPlcBlendingLow`=2, `PpcmPlcBlendingPrev`=3, `PpcmPlcBlendingNext`=4, `PpcmPlcBlendingHigh`=5; default 3 |
| `Trajectory.points[n].transitionMode` | `blendingEnumId` (PLCopen `MC_TRANSITION_MODE` int) | `PpcmPlcTMNone`=0, `PpcmPlcTMCornerDist`=1, `PpcmPlcTMConstVel`=2, `PpcmPlcTMStartVel`=3, `PpcmPlcTMMaxVel`=4; default 0 |

Example recipe output for the trajectory seed (`TRJ_RULESET_1_01`):

```text
Trajectory.trajectoryId:=TRJ_TEST_01_01
Trajectory.pointCount:=2
Trajectory.points[0].blendingMode:=10.000000
Trajectory.points[0].bufferMode:=3
Trajectory.points[0].hasBlendingOverride:=FALSE
Trajectory.points[0].isBreakPoint:=FALSE
Trajectory.points[0].pos.v[0]:=100.000000
Trajectory.points[0].pos.v[1]:=200.000000
Trajectory.points[0].pos.v[2]:=300.000000
Trajectory.points[0].sequenceNum:=1
Trajectory.points[0].timeOffsetMs:=500.000000
Trajectory.points[0].transitionMode:=0
Trajectory.points[1].blendingMode:=0.000000
Trajectory.points[1].bufferMode:=3
Trajectory.points[1].hasBlendingOverride:=FALSE
Trajectory.points[1].isBreakPoint:=TRUE
...
```

The `blendingEnumId` field on `TrajectoryPoint` encodes either a `bufferMode` or a `transitionMode` override using PLCopen-aligned enumerations from `moqui-math` (`PpcmPlcAborting` through `PpcmPlcTMMaxVel`). When `blendingEnumId` is `NULL`, both modes fall back to their defaults (`bufferMode=3` = blending previous, `transitionMode=0` = none).

Example REST call (trajectory export):

```bash
curl -X POST \
  http://localhost:8081/api/device-config/export \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: change-me' \
  -d '{
    "deviceRuleSetId":"TRJ_RULESET_1_01"
  }'
```

### Scenario F — Device content transfer

Use content transfer for file-like payloads such as recipes, device files, or other content that must be sent to a configured destination.

The destination path is **not** taken from `DeviceContent.contentLocation`.
That field is only the Moqui-side source location of the file to read.

The effective transfer destination is the gateway-side `DeviceRequest.brokerUri`
resolved by the `{requestName}` passed to:

```text
POST /api/device-content/transfer/{requestName}
```

The flow is:

1. Moqui reads the file from `DeviceContent.contentLocation`
2. Moqui sends `filename` + `contentBase64` to the gateway REST endpoint
3. the gateway loads the gateway-side `DeviceRequest`
4. the gateway writes the file to `DeviceRequest.brokerUri`

Typical `brokerUri` examples for content transfer are:

- `file:/mnt/cnc-share/programs`
- `file:/var/lib/plc/recipes`
- `sftp:operator@cnc1.factory.local:22/programs`

This means the final file path or transfer endpoint used by **Transfer Content**
is always the one declared in the gateway-side `DeviceRequest.brokerUri`.

Example:

```bash
curl -X POST \
  http://localhost:8081/api/device-content/transfer/SFTP_TRANSFER_REQ \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: change-me' \
  -d '{
    "filename":"recipe.txt",
    "contentBase64":"SGVsbG8K"
  }'
```

---

## REST API

All REST endpoints are under:

```text
/api
```

When authentication is enabled, pass the API token with either:

```text
X-API-Key: <token>
```

or:

```text
Authorization: Bearer <token>
```

| Endpoint | Purpose |
|---|---|
| `POST /api/device-request/run/{requestName}` | Execute a `DeviceRequest` already defined in the database, including unsubscribe requests modeled as `DrtUnsubscribe`. |
| `POST /api/device-config/export` | Export device configuration / recipe data for a device workflow. |
| `POST /api/device-content/transfer/{requestName}` | Transfer configured file-like device content. |

### Run a DeviceRequest

```bash
curl -X POST \
  http://localhost:8081/api/device-request/run/{requestName} \
  -H 'X-API-Key: change-me'
```

This endpoint executes a `DeviceRequest` already defined in the database. It does not define the request.

The gateway enforces scope. A request can run only if its `DEVICE_ID` belongs to a `DeviceGroup` served by the configured `GATEWAY_DEVICE_ID`.

### Unsubscribe

Unsubscribe is not exposed through a separate REST path in the current gateway implementation.

To stop a runtime subscription route, create or use a `DeviceRequest` whose `REQUEST_TYPE_ENUM_ID` is `DrtUnsubscribe`, then execute it with:

```bash
curl -X POST \
  http://localhost:8081/api/device-request/run/{unsubscribeRequestName} \
  -H 'X-API-Key: change-me'
```

At runtime, the `run/{requestName}` endpoint dispatches that request to the unsubscribe flow.

It does not permanently deactivate the `DeviceRequest`.

To prevent a startup subscription from being restored after restart, update the persistent model in the Moqui database. Common options are:

- set `DeviceRequest.thruDate`;
- change `DeviceRequest.routerEnumId`;
- remove or disable the relevant `DeviceGroupMember` association.

For request bodies and longer examples, see the running scenarios above.

---

## Testing

### Fast/local tests

```bash
./gradlew test
```

These tests should not require a complete Moqui runtime.

### Integration tests

Integration tests require external services such as PostgreSQL, MQTT broker, and OPC UA test infrastructure.

The standard local MQTT broker for integration tests is ActiveMQ Artemis from:

```bash
docker compose -f ../moqui-framework/docker/activemq-compose.yml -p moqui-gateway up -d
```

Run examples:

```bash
./gradlew integrationTest --tests '*MqttInboundIntegrationTest' -Dquarkus.profile=integration
./gradlew integrationTest --tests '*GatewaySeededRouteIntegrationTest' -Dquarkus.profile=integration
./gradlew integrationTest --tests '*OpcUaGatewayIntegrationTest' -Dquarkus.profile=integration
./gradlew integrationTest --tests '*PlcLogIngestIntegrationTest' -Dquarkus.profile=integration
```

Or use:

```bash
./scripts/run-integration-tests.sh
```

Before considering the component production-ready for a plant, run at least:

- startup restore from DB;
- MQTT write;
- MQTT subscribe;
- gateway crash/restart;
- broker down/up;
- OPC UA read/write/subscribe if used;
- PLC log ingestion if used;
- readiness behavior with broker/PLC temporarily unreachable.

### Important note about test SQL

`src/test/resources/db/init.sql` is a minimal PostgreSQL schema used by automated tests and local examples.

It is **not** the production schema and it is **not** the canonical `moqui-device` data model.

In production, tables and seed data are managed by Moqui and by the `moqui-device` component.

### Seed examples

`src/test/resources/device-gateway-seed.sql` contains local MQTT-oriented test data.

`src/test/resources/device-gateway-opcua-seed.sql` contains local OPC UA-oriented test data.

They are useful examples, but production seed data should live in Moqui component data files or be inserted through Moqui services/screens.

---

## Docker Compose

This repository includes simple local Compose files:

```text
docker/postgres-compose.yml
docker/device-gateway-compose.yml
```

The broker used by the standard local setup is still ActiveMQ Artemis from the Moqui framework Docker directory:

```bash
docker compose -f ../moqui-framework/docker/activemq-compose.yml -p moqui-gateway up -d
```

Start PostgreSQL:

```bash
docker compose -f docker/postgres-compose.yml -p moqui-gateway up -d
```

The PostgreSQL Compose file auto-loads the local test schema and seed on first startup. If the named volume already exists, initialization is not repeated.

Build and start the gateway:

```bash
docker compose -f docker/device-gateway-compose.yml -p moqui-gateway up -d --build
```

To build the gateway container with the native Dockerfile:

```bash
DEVICE_GATEWAY_DOCKERFILE=src/main/docker/Dockerfile.graalvm \
DEVICE_GATEWAY_IMAGE=moqui-device-gateway:native \
docker compose -f docker/device-gateway-compose.yml -p moqui-gateway up -d --build
```

Check readiness:

```bash
curl http://localhost:8081/q/health/ready
```

Stop services:

```bash
docker compose -f docker/device-gateway-compose.yml -p moqui-gateway down
docker compose -f docker/postgres-compose.yml -p moqui-gateway down
```

Remove volumes too:

```bash
docker compose -f docker/device-gateway-compose.yml -p moqui-gateway down -v
docker compose -f docker/postgres-compose.yml -p moqui-gateway down -v
```

The local Compose files are for development and simple testing. Production deployments should use real Moqui database infrastructure, secrets, backups, network policy, and monitoring.

---

## Docker Swarm deployment

For Swarm, configure the logical gateway identity explicitly:

```yaml
environment:
  - GATEWAY_DEVICE_ID=GW_EDGE_01
```

Rules:

- use one active replica per logical `GATEWAY_DEVICE_ID`;
- multiple simultaneous gateways should be modeled as different `PhysicalDevice` gateway records;
- assign each gateway to its own `DeviceGroup` scope or to clearly controlled shared scopes;
- do not run two active containers with the same `GATEWAY_DEVICE_ID` unless you add an external leader-election or lease mechanism;
- Swarm failover means the same logical gateway is restarted on another node;
- local `/deployments/data` is not the subscription source of truth;
- startup subscriptions are restored from the Moqui database.

Example logical layout:

```text
GW_EDGE_01 -> Line 1 PLCs
GW_EDGE_02 -> Line 2 PLCs
GW_EDGE_03 -> Drying/Maturation cells
```

If this component is checked out inside a larger deployment repository, that repository may provide production-oriented Swarm files with Docker secrets, configs, shared volumes, restart policy, and update policy.

---

## MQTT persistent subscriptions

The gateway can restore subscription routes from the database after restart.

This is not the same thing as MQTT broker-side persistent session delivery.

### DB-driven startup restore

DB-driven startup restore means:

```text
DeviceRequest in DB -> gateway startup discovery -> Camel subscription route
```

It recreates the route after gateway restart.

### MQTT broker-side persistent session

Broker-side persistent MQTT delivery determines whether messages published while the gateway was offline can be delivered after reconnect.

For broker-side persistent subscriptions, all of these must be true:

- stable MQTT `clientId`;
- `cleanStart=false`;
- `sessionExpiryInterval > 0`;
- QoS suitable for the required delivery semantics;
- broker persistence enabled/configured.

When the MQTT URI does not already define a `clientId`, the gateway generates a stable one based on:

```text
GATEWAY_DEVICE_ID + requestName + item index
```

Example:

```text
moqui-gw-GW_EDGE_01-VPL_MQTT_SUB_REQ_01-0
```

Do not use container ID or hostname as MQTT `clientId` if the subscription must survive Swarm failover. Those values can change when the container is rescheduled.

---

## Health check

Use:

```text
/q/health/ready
```

Readiness checks whether the gateway process can operate as a gateway process.

It checks:

- process is running;
- Camel context is started;
- database is reachable;
- `GATEWAY_DEVICE_ID` is configured;
- the gateway exists as `Device` + `PhysicalDevice`;
- the gateway belongs to at least one `DeviceGroup` as `DgmpEdgeGateway`.

Readiness does **not** mean every PLC, MQTT broker, or OPC UA server is currently reachable.

Readiness should not go `DOWN` only because a remote PLC or MQTT broker is temporarily unavailable. Field-level failures should be handled through MQTT Last Will messages, route error handling, logs, and optional diagnostic records in Moqui.

---

## Security notes

For production:

- replace `GATEWAY_API_AUTH_TOKEN` with a real secret;
- do not deploy with `change-me` or `change-me-in-production`;
- use Docker secrets or an equivalent secret manager;
- restrict network exposure of port `8081`;
- expose the gateway only to trusted OT/IT networks;
- use TLS where required by the plant network policy;
- monitor logs and failed route starts.

---

## Troubleshooting

| Symptom | Likely cause | What to check |
|---|---|---|
| `/q/health/ready` is `DOWN` | Missing or invalid gateway identity | Check `GATEWAY_DEVICE_ID`, `Device`, `PhysicalDevice`, `DeviceGroupMember`. |
| Gateway starts but restores no routes | Gateway is not linked to PLCs through a valid group | Check `DgmpEdgeGateway`, target PLC membership, `STATUS_ID`, `routerEnumId`, and dates. |
| Manual request is rejected | Request belongs to a PLC outside this gateway scope | Check `DeviceRequest.deviceId` and `DeviceGroupMember` associations. |
| Request returns an error or no route starts | Invalid broker URI, topic, OPC UA node, connection, or Camel endpoint | Check `DeviceRequest`, `DeviceRequestItem`, `DeviceConnection`, and logs. |
| MQTT messages are not received after restart | Broker persistent session is not configured | Check `clientId`, `cleanStart`, `sessionExpiryInterval`, QoS, and broker persistence. |
| Inbound value is not stored | Payload does not match expected parameter mapping | Check payload, `parameterId`, `Parameter`, and `ParameterLog`. |
| Duplicate MQTT subscriptions | Two active containers use the same `GATEWAY_DEVICE_ID` | Use one active replica per gateway ID unless leader election is added. |
| Local Docker gateway is not ready | Test schema or seed not loaded | Load `init.sql`, adapt/load seed data, and verify `GW_EDGE_01` exists. |

Useful SQL checks:

```sql
SELECT * FROM DEVICE WHERE DEVICE_ID = 'GW_EDGE_01';
SELECT * FROM PHYSICAL_DEVICE WHERE DEVICE_ID = 'GW_EDGE_01';
SELECT * FROM DEVICE_GROUP_MEMBER WHERE MEMBER_DEVICE_ID = 'GW_EDGE_01';
SELECT * FROM DEVICE_REQUEST ORDER BY REQUEST_NAME;
SELECT * FROM DEVICE_REQUEST_ITEM ORDER BY REQUEST_NAME, SEQUENCE_NUM;
SELECT * FROM PARAMETER ORDER BY PARAMETER_ID;
SELECT * FROM PARAMETER_LOG ORDER BY OBSERVED_DATE DESC;
```

---

## Scope and limitations

This README describes operational usage, model configuration, local testing, and deployment principles.

It intentionally does not document every internal Java class or every Camel route implementation detail.

The important operational rule is:

```text
The model defines what the gateway may do.
The REST API and startup discovery activate what the model declares.
Camel routes execute the model at runtime.
```
