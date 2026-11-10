# Differences from PLC4J and the Camel gateway

The drivers of this library replace the protocol part of moqui-plc4j
(`FieldbusClient`, `Plc4jUtil`) and of the Quarkus/Camel gateway
(`GatewayRequestService` and its routes). Which service of moqui-device took over
each Camel route is listed in moqui-device's
[SERVICE-PORT.md](https://github.com/moqui/moqui-device/blob/mr-moqui/SERVICE-PORT.md).

## Differences requiring review

* A `DrtEvent`/`DrtStateChange` subscription requires an actual event backend;
  it is not silently converted into Modbus polling. Use `DrtCyclic` explicitly.
* Modbus reads share a response across indexed bulk items. Bulk writes require
  complete, unique indices for each configured array and produce one ordered
  PDU per bulk address. All values and PDU limits are checked before connecting.
* Numeric conversions use Decimal, zero bounds and zero significant digits are
  meaningful, and zero scaling is rejected. Out-of-range writes and fractional
  writes to integer physical types fail instead of truncating.
* MQTT supports 3.1.1 and 5, QoS 0/1/2, stable subscription client IDs,
  persistent sessions and cancellation-aware reconnect delays. A call never
  replays a write: retries are left to the caller, and the receiver must be
  idempotent. The stable ID format differs from Camel's; migrating an old session
  may require broker cleanup.
* MQTT TLS supports system roots, a configured CA and paired client certificate
  and key. These options still require deployment-specific TLS tests. OPC UA
  rejects untrusted certificates and deprecated security policies.
* An explicit MQTT stop attempts acknowledged UNSUBSCRIBE for every topic.
  An unavailable broker can leave persistent remote subscriptions behind;
  shutdown does not claim that local cancellation guarantees remote cleanup.

## Next increments

1. Validate the Modbus write limits and exception handling against the target
   devices of an installation.
2. Private CA/mTLS for MQTT and secured OPC UA need installation-specific
   acceptance; so does persistent-session cleanup on the broker.
3. PROFINET, EtherNet/IP, EtherCAT and Modbus TLS are future backends. Their
   libraries, licensing, role and hardware requirements need review. Cyclic
   hard-real-time I/O and safety PLC control are not established by this library.

GenICam remains a separate camera implementation of
`moqui.device.DeviceServices.run#DeviceRequestInternal`.
