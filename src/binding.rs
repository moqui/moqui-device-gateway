/*
 * This software is in the public domain under CC0 1.0 Universal plus a
 * Grant of Patent License.
 *
 * To the extent possible under law, the author(s) have dedicated all
 * copyright and related and neighboring rights to this software to the
 * public domain worldwide. This software is distributed without any
 * warranty.
 *
 * You should have received a copy of the CC0 Public Domain Dedication
 * along with this software (see the LICENSE.md file). If not, see
 * <http://creativecommons.org/publicdomain/zero/1.0/>.
 */

//! The Moqui binding (feature `moqui`): run#DeviceGatewayRequest, the implementation of
//! moqui.device.DeviceServices.run#DeviceRequestInternal, as Plc4jServices.run#Plc4jRequest was for PLC4J. The
//! DeviceRequest, its device, connection and items become a request of the facade, which runs it on the driver of the
//! connection. The values read or received go to the store services of moqui-device, which own the persistence. No
//! entity or service reaches the drivers.

use crate::{codec::ValueCodec, model::*, tool::DeviceGatewayToolFactory};
use moqui_core::{
    context::ExecutionContext,
    entity::{EntityFacade, EntityValue},
    native::{Parameters, ServiceInfo},
};
use moqui_lib::{error::Error, object::Value};
use rust_decimal::Decimal;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn error(e: impl std::fmt::Display) -> Error {
    Error::new(e.to_string())
}

pub(crate) fn text(v: &EntityValue, key: &str) -> Option<String> {
    v.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(String::from)
}

fn decimal(v: &EntityValue, key: &str) -> Result<Option<Decimal>, Error> {
    v.get(key)
        .filter(|v| !matches!(v, Value::Null))
        .map(|v| match v {
            Value::Decimal(d) => Ok(*d),
            Value::I64(n) => Ok(Decimal::from(*n)),
            Value::I32(n) => Ok(Decimal::from(*n)),
            Value::String(s) => s.parse().map_err(error),
            _ => Err(Error::new(format!("{key} must be an exact decimal"))),
        })
        .transpose()
}

fn positive_ms(v: &EntityValue, key: &str, default: u64) -> Result<Duration, Error> {
    let n = decimal(v, key)?.unwrap_or(Decimal::from(default));
    let ms = u64::try_from(n).map_err(error)?;
    if ms == 0 || ms > 86_400_000 || !n.fract().is_zero() {
        return Err(Error::new(format!("{key} must be 1..86400000 milliseconds")));
    }
    Ok(Duration::from_millis(ms))
}

/// An entity value as a service receives it: the EntityValue of a Rust caller, or the value that a script or XML
/// actions share with their context (a Mutex around it). The request is prepared from a copy.
fn entity_value(v: &Value) -> Option<Arc<EntityValue>> {
    let Value::Object(object) = v else {
        return None;
    };
    object.downcast::<EntityValue>().or_else(|| {
        object
            .downcast::<std::sync::Mutex<EntityValue>>()
            .map(|cell| Arc::new(moqui_lib::sync::lock(&cell).clone()))
    })
}

fn entity(params: &Parameters, key: &str) -> Result<Arc<EntityValue>, Error> {
    match params.get(key) {
        None | Some(Value::Null) => Err(Error::new(format!("Missing {key}"))),
        Some(v) => entity_value(v).ok_or_else(|| Error::new(format!("{key} must be EntityValue"))),
    }
}

/// A list of entity values: a list, the list of a script, or a find result.
fn entity_list(params: &Parameters, key: &str) -> Result<Vec<Arc<EntityValue>>, Error> {
    match params.get(key) {
        None | Some(Value::Null) => Ok(vec![]),
        Some(Value::List(l)) => l
            .iter()
            .map(|v| entity_value(v).ok_or_else(|| Error::new("Item must be EntityValue")))
            .collect(),
        Some(v @ Value::Object(object)) => {
            if let Some(values) = moqui_core::script::script_list_values(v) {
                return Ok(values.into_iter().map(Arc::new).collect());
            }
            object
                .downcast::<moqui_core::entity::EntityList>()
                .map(|list| list.iter().cloned().map(Arc::new).collect())
                .ok_or_else(|| Error::new(format!("{key} must be a list of entity values")))
        }
        Some(_) => Err(Error::new(format!("{key} must be a list of entity values"))),
    }
}

pub(crate) fn facade(ec: &ExecutionContext) -> Result<EntityFacade, Error> {
    ec.factory()
        .entity()
        .ok_or_else(|| Error::new("Entity Engine is not installed"))
}

/// A record by its primary key, from the entity cache.
fn find(ef: &EntityFacade, entity: &str, key: &str, id: &str) -> Result<Arc<EntityValue>, Error> {
    ef.find(entity)?
        .eq(key, Value::string(id))
        .use_cache(true)
        .one()?
        .map(Arc::new)
        .ok_or_else(|| Error::new(format!("{entity} record not found: {id}")))
}

fn parse_options(raw: &str) -> Result<BTreeMap<String, String>, Error> {
    if raw.trim().starts_with('{') {
        return serde_json::from_str(raw).map_err(error);
    }
    let mut result = BTreeMap::new();
    for (k, v) in url::form_urlencoded::parse(raw.trim_start_matches('?').as_bytes()) {
        if result.insert(k.into_owned(), v.into_owned()).is_some() {
            return Err(Error::new("Duplicate connection option"));
        }
    }
    Ok(result)
}

fn connection(r: &EntityValue, c: Option<&EntityValue>, device_id: &str) -> Result<Connection, Error> {
    let mut options = if let Some(c) = c {
        if text(c, "deviceId").as_deref() != Some(device_id) {
            return Err(Error::new("Connection belongs to another device"));
        }
        if text(c, "userId").is_some() {
            return Err(Error::new(
                "DeviceConnection.userId needs an explicit credential provider; password hashes cannot authenticate a \
                 device",
            ));
        }
        parse_options(&text(c, "options").unwrap_or_default())?
    } else {
        BTreeMap::new()
    };
    let endpoint = text(r, "brokerUri")
        .or_else(|| c.and_then(|c| text(c, "transportConfig")))
        .ok_or_else(|| Error::new("No field connection endpoint"))?;
    let driver = c.and_then(|c| text(c, "driverEnumId"));
    let protocol = if endpoint.starts_with("paho-mqtt5:")
        || endpoint.starts_with("paho:")
        || endpoint.starts_with("mqtt://")
        || endpoint.starts_with("mqtts://")
        || driver.as_deref() == Some("DcdMqtt")
    {
        Protocol::Mqtt
    } else if endpoint.starts_with("milo-client:")
        || endpoint.starts_with("opc.tcp://")
        || driver.as_deref() == Some("DcdOpcUa")
    {
        Protocol::OpcUa
    } else if driver.as_deref() == Some("DcdModbusTCP") || endpoint.starts_with("modbus-tcp://") {
        Protocol::ModbusTcp
    } else if matches!(driver.as_deref(), Some("DcdLogixCIPEIP" | "DcdEthernetIP"))
        || endpoint.starts_with("logix://")
        || endpoint.starts_with("eip://")
    {
        Protocol::EthernetIp
    } else if driver.as_deref() == Some("DcdEtherCAT") || endpoint.starts_with("ethercat://") {
        Protocol::EtherCat
    } else {
        return Err(Error::new(
            "Unsupported fieldbus protocol; no default backend is selected",
        ));
    };
    let mut endpoint = endpoint;
    let legacy_mqtt_version = if endpoint.starts_with("paho:") { "3" } else { "5" };
    if let Some(rest) = endpoint
        .strip_prefix("paho-mqtt5:")
        .or_else(|| endpoint.strip_prefix("paho:"))
    {
        let (_, query) = rest
            .split_once('?')
            .ok_or_else(|| Error::new("Legacy MQTT endpoint requires brokerUrl"))?;
        for (k, v) in parse_options(query)? {
            if options.insert(k, v).is_some() {
                return Err(Error::new("Duplicate MQTT connection option"));
            }
        }
        endpoint = options
            .remove("brokerUrl")
            .ok_or_else(|| Error::new("Legacy MQTT endpoint requires brokerUrl"))?;
        endpoint = endpoint
            .replacen("tcp://", "mqtt://", 1)
            .replacen("ssl://", "mqtts://", 1);
        options
            .entry("protocol-version".into())
            .or_insert(legacy_mqtt_version.into());
    }
    if protocol == Protocol::Mqtt {
        endpoint = endpoint
            .replacen("tcp://", "mqtt://", 1)
            .replacen("ssl://", "mqtts://", 1);
        if !endpoint.contains("://") {
            endpoint = format!("mqtt://{endpoint}");
        }
        for (legacy, native) in [
            ("clientId", "client-id"),
            ("cleanStart", "clean-start"),
            ("automaticReconnect", "automatic-reconnect"),
            ("sessionExpiryInterval", "session-expiry-seconds"),
            ("maxReconnectDelay", "max-reconnect-delay-ms"),
            ("receiveMaximum", "receive-maximum"),
            ("keepAliveInterval", "keep-alive-seconds"),
            ("retained", "retain"),
        ] {
            if let Some(value) = options.remove(legacy) {
                if options.insert(native.into(), value).is_some() {
                    return Err(Error::new(format!("Duplicate MQTT option: {native}")));
                }
            }
        }
        if let Some(qos) = decimal(r, "qos")? {
            options.insert("qos".into(), qos.to_string());
        }
        if let Some(retained) = text(r, "retained") {
            options.insert("retain".into(), if retained == "Y" { "true" } else { "false" }.into());
        }
        let gateway = gateway_device_id();
        if !gateway.is_empty() {
            options
                .entry("client-id-prefix".into())
                .or_insert(format!("moqui-gw-{gateway}"));
        }
    }
    if let Some(rest) = endpoint.strip_prefix("milo-client:") {
        endpoint = rest.to_string();
    }
    if protocol == Protocol::OpcUa {
        if let Some((url, query)) = endpoint.clone().split_once('?') {
            for (k, v) in parse_options(query)? {
                if options.insert(k, v).is_some() {
                    return Err(Error::new("Duplicate OPC UA option"));
                }
            }
            endpoint = url.to_string();
        }
    }
    let username = options.remove("username").or_else(|| options.remove("userName"));
    let password = options.remove("password");
    let resolve = |value: Option<String>, env: Option<String>| -> Result<Option<String>, Error> {
        match (value, env) {
            (Some(_), Some(_)) => Err(Error::new(
                "Specify a credential value or environment variable, not both",
            )),
            (Some(v), None) => Ok(Some(v)),
            (None, Some(k)) => std::env::var(k)
                .map(Some)
                .map_err(|_| Error::new("Configured credential environment variable is missing")),
            _ => Ok(None),
        }
    };
    let username = resolve(username, options.remove("usernameEnv"))?;
    let password = resolve(password, options.remove("passwordEnv"))?;
    Ok(Connection {
        name: c
            .and_then(|c| text(c, "connectionName"))
            .unwrap_or_else(|| "broker".into()),
        protocol,
        endpoint,
        options,
        credentials: Credentials { username, password },
    })
}

fn kind(ef: &EntityFacade, def: &EntityValue) -> Result<ParameterKind, Error> {
    let ty = text(def, "parameterTypeEnumId").ok_or_else(|| Error::new("ParameterDef has no type"))?;
    Ok(match ty.as_str() {
        "PtTextIndicator" => ParameterKind::Indicator,
        "PtByte" | "PtBitSet" | "PtEnumeration" => ParameterKind::Bits,
        _ => {
            let enum_value = find(ef, "moqui.basic.Enumeration", "enumId", &ty)?;
            if ty == "PtNumber" || text(&enum_value, "parentEnumId").as_deref() == Some("PtNumber") {
                ParameterKind::Numeric
            } else {
                ParameterKind::Text
            }
        }
    })
}

/// The physical type of an item without itemTypeEnumId, from the kind of its parameter: what a JSON (MQTT) value
/// carries. Set itemTypeEnumId for an exact register or PLC type.
fn default_physical_type(kind: ParameterKind) -> PhysicalType {
    match kind {
        ParameterKind::Numeric => PhysicalType::F64,
        ParameterKind::Indicator => PhysicalType::Bool,
        ParameterKind::Bits => PhysicalType::U64,
        ParameterKind::Text => PhysicalType::Text,
    }
}

fn logical_value(parameter: &EntityValue, kind: ParameterKind) -> Result<FieldValue, Error> {
    Ok(match kind {
        ParameterKind::Numeric => FieldValue::Decimal(
            decimal(parameter, "numericValue")?.ok_or_else(|| Error::new("Parameter has no numeric value"))?,
        ),
        ParameterKind::Indicator => match text(parameter, "symbolicValue").as_deref() {
            Some("Y") => FieldValue::Bool(true),
            Some("N") => FieldValue::Bool(false),
            _ => return Err(Error::new("Indicator must be Y or N")),
        },
        ParameterKind::Bits => {
            let raw = text(parameter, "parameterEnumId").ok_or_else(|| Error::new("Parameter has no bit value"))?;
            let bits = raw.strip_prefix("0b").unwrap_or(&raw).replace([' ', '_'], "");
            if bits.is_empty() || !bits.chars().all(|c| c == '0' || c == '1') {
                return Err(Error::new("Bit parameter must contain a binary string"));
            }
            FieldValue::Unsigned(u64::from_str_radix(&bits, 2).map_err(error)?)
        }
        ParameterKind::Text => FieldValue::Text(
            text(parameter, "symbolicValue")
                .or_else(|| text(parameter, "textValue"))
                .ok_or_else(|| Error::new("Parameter has no text value"))?,
        ),
    })
}

/// The service a request runs through: run#DeviceGatewayRequest takes the driver of the connection
/// (DeviceConnection.driverEnumId, else the scheme of the endpoint); run#PlcLogRequest is an MQTT subscription whose
/// events are stored as logs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceKind {
    Fieldbus,
    PlcLog,
}

pub fn prepare(
    ec: &ExecutionContext,
    params: &Parameters,
    service: ServiceKind,
) -> Result<(Arc<PreparedDeviceRequest>, RequestInput), Error> {
    let r = entity(params, "request")?;
    let ef = facade(ec)?;
    let ef = &ef;
    if ef.sim_active() {
        return Err(Error::new("Device gateway network I/O is disabled in simulation mode"));
    }
    let device_id = text(&r, "deviceId").ok_or_else(|| Error::new("Request has no deviceId"))?;
    let device = entity(params, "device")?;
    if text(&device, "deviceId").as_deref() != Some(&device_id) {
        return Err(Error::new("Request and device do not match"));
    }
    let c = params
        .get("connection")
        .filter(|v| !matches!(v, Value::Null))
        .map(|_| entity(params, "connection"))
        .transpose()?;
    let request_type = text(&r, "requestTypeEnumId").unwrap_or_default();
    let mut operation = Operation::from_enum(&request_type).map_err(error)?;
    // A DrtRead with a pollingInterval polls until a DrtUnsubscribe.
    if operation == Operation::Read && decimal(&r, "pollingInterval")?.is_some() {
        operation = Operation::Poll;
    }
    let name = text(&r, "requestName").ok_or_else(|| Error::new("Missing requestName"))?;
    let connection = if operation == Operation::Unsubscribe {
        let parent_name =
            text(&r, "parentRequestName").ok_or_else(|| Error::new("Unsubscribe requires parentRequestName"))?;
        let parent = find(ef, "moqui.device.DeviceRequest", "requestName", &parent_name)?;
        if text(&parent, "deviceId").as_deref() != Some(&device_id) {
            return Err(Error::new("Parent subscription belongs to another device"));
        }
        let parent_connection = text(&parent, "connectionName")
            .map(|name| find(ef, "moqui.device.DeviceConnection", "connectionName", &name))
            .transpose()?;
        connection(&parent, parent_connection.as_deref(), &device_id)?
    } else {
        connection(&r, c.as_deref(), &device_id)?
    };
    if service == ServiceKind::PlcLog && connection.protocol != Protocol::Mqtt {
        return Err(Error::new(format!("PLC log request {name} needs an MQTT endpoint")));
    }
    check_gateway_scope(ec, &name, &device_id)?;
    if service == ServiceKind::PlcLog && !matches!(operation, Operation::Subscribe | Operation::Unsubscribe) {
        return Err(Error::new(
            "A PLC log request is a subscription (DrtSubscribe or DrtEvent) or DrtUnsubscribe",
        ));
    }
    let mut request = PreparedDeviceRequest {
        name,
        device_id: device_id.clone(),
        parent_request: text(&r, "parentRequestName"),
        operation,
        subscription_type: SubscriptionType::from_enum(&request_type),
        connection,
        query: text(&r, "query"),
        items: vec![],
        timeout: positive_ms(&r, "timeout", 5000)?,
        polling_interval: positive_ms(&r, "pollingInterval", 1000)?,
        logging: text(&r, "purposeEnumId").as_deref() == Some("DrpLogging"),
        plc_log: service == ServiceKind::PlcLog,
        only_changed: text(&r, "onlyChangedParameters").as_deref() != Some("N"),
    };
    let mut input = RequestInput::default();
    for (list_name, bulk) in [("bulkRequestItemList", true), ("requestItemList", false)] {
        for (index, i) in entity_list(params, list_name)?.into_iter().enumerate() {
            if text(&i, "requestName").as_deref() != Some(&request.name) {
                return Err(Error::new("Item belongs to another request"));
            }
            let id = text(&i, "parameterId").ok_or_else(|| Error::new("Item has no parameterId"))?;
            let p = find(ef, "moqui.math.Parameter", "parameterId", &id)?;
            if text(&p, "deviceId").as_deref() != Some(&device_id) {
                return Err(Error::new("Parameter does not belong to the request device"));
            }
            let def = find(
                ef,
                "moqui.math.ParameterDef",
                "parameterDefId",
                &text(&p, "parameterDefId").ok_or_else(|| Error::new("Parameter has no definition"))?,
            )?;
            let kind = kind(ef, &def)?;
            let ty = match text(&i, "itemTypeEnumId") {
                Some(t) => PhysicalType::parse(&t).map_err(error)?,
                None => default_physical_type(kind),
            };
            let digits = decimal(&i, "significantDigits")?
                .map(|d| u32::try_from(d).map_err(error))
                .transpose()?;
            let codec = ValueCodec {
                physical_min: decimal(&i, "minItemValue")?,
                physical_max: decimal(&i, "maxItemValue")?,
                logical_min: decimal(&def, "minValue")?,
                logical_max: decimal(&def, "maxValue")?,
                scale: decimal(&i, "scalingFactor")?.unwrap_or(Decimal::ONE),
                offset: decimal(&i, "offsetValue")?.unwrap_or(Decimal::ZERO),
                digits,
                tolerance: decimal(&i, "tolerance")?.unwrap_or(Decimal::ZERO),
                reverse_logic: text(&i, "reverseLogic").as_deref() == Some("Y"),
                allow_duplicate: text(&i, "allowDuplicate").as_deref() == Some("Y"),
            };
            if operation == Operation::Write {
                input.values.insert(id.clone(), logical_value(&p, kind)?);
            }
            request.items.push(Item {
                parameter_id: id,
                name: text(&i, "requestItemName").unwrap_or_default(),
                address: text(&i, "query").or_else(|| request.query.clone()).unwrap_or_default(),
                physical_type: ty,
                kind,
                codec,
                bulk_index: if bulk && request.connection.protocol == Protocol::ModbusTcp {
                    Some(index)
                } else {
                    None
                },
            });
        }
    }
    match params.get("payload") {
        None | Some(Value::Null) => {}
        Some(payload @ Value::Map(_)) => {
            if request.operation != Operation::Write || request.connection.protocol != Protocol::Mqtt {
                return Err(Error::new("A payload is sent only by an MQTT write (DrtWrite)"));
            }
            if !request.items.is_empty() {
                return Err(Error::new(
                    "A write sends either a payload or its request items, not both",
                ));
            }
            input.payload = Some(serde_json::to_vec(&moqui_lib::json::to_json(payload)).map_err(error)?);
        }
        Some(_) => return Err(Error::new("payload must be a map")),
    }
    request.validate().map_err(error)?;
    Ok((Arc::new(request), input))
}

/// moqui.device.gateway.DeviceGatewayServices.run#DeviceGatewayRequest: the driver of the connection runs the request.
pub fn run_device_gateway_request(
    ec: &ExecutionContext,
    _: &ServiceInfo<'_>,
    params: &Parameters,
) -> Result<Parameters, Error> {
    run_request(ec, params, ServiceKind::Fieldbus)
}

/// moqui.device.gateway.DeviceGatewayServices.run#PlcLogRequest: an MQTT subscription to PLC log events.
pub fn run_plc_log_request(
    ec: &ExecutionContext,
    _: &ServiceInfo<'_>,
    params: &Parameters,
) -> Result<Parameters, Error> {
    run_request(ec, params, ServiceKind::PlcLog)
}

fn run_request(ec: &ExecutionContext, params: &Parameters, service: ServiceKind) -> Result<Parameters, Error> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(Error::new(
            "Invoke the Moqui service on a service worker; async Rust callers should use DeviceGatewayFacade directly",
        ));
    }
    let (request, input) = prepare(ec, params, service)?;
    let gateway = gateway(ec)?;
    let mut output = RequestOutput::default();
    let execution = gateway.block_on(gateway.facade().execute(
        request.clone(),
        &input,
        &mut output,
        request.deadline(),
        Some(gateway.sink()),
    ));
    let status = execution.as_ref().copied().unwrap_or_else(|e| {
        if e.may_have_applied {
            ExecutionOutcome::Uncertain
        } else if output
            .items
            .iter()
            .any(|i| matches!(i.status, ItemStatus::Completed | ItemStatus::Unchanged))
        {
            ExecutionOutcome::Partial
        } else {
            ExecutionOutcome::Failed
        }
    });
    // A read stores its values, as FieldbusClient.read of moqui-plc4j did; subscriptions and polling store through
    // the sample queue.
    let read_samples = if request.operation == Operation::Read {
        output
            .items
            .iter()
            .filter(|i| i.status == ItemStatus::Completed && i.value.is_some())
            .map(|i| Sample {
                device_id: request.device_id.clone(),
                log_value_kind: None,
                parameter_id: Some(i.parameter_id.clone()),
                value: i.value.clone().unwrap_or(FieldValue::Null),
                source_time: i.source_time,
                received_time: SystemTime::now(),
                quality: i.quality.clone(),
            })
            .collect::<Vec<_>>()
    } else {
        vec![]
    };
    let samples = read_samples
        .iter()
        .map(sample_to_value)
        .collect::<Result<Vec<_>, _>>()?;
    let stored_count = if samples.is_empty() {
        Value::I64(0)
    } else {
        let stored = call_store(ec, STORE_SAMPLES, &request.name, request.logging, samples.clone(), &[])?;
        stored.get("storedCount").cloned().unwrap_or(Value::I64(0))
    };
    let mut result = Parameters::new();
    result.insert("status".into(), Value::string(status.as_str()));
    result.insert("samples".into(), Value::List(samples));
    result.insert("storedCount".into(), stored_count);
    result.insert(
        "itemResults".into(),
        Value::List(output.items.iter().map(item_result_value).collect()),
    );
    result.insert(
        "subscriptionId".into(),
        output.subscription_id.map(Value::string).unwrap_or(Value::Null),
    );
    result.insert(
        "errorMessage".into(),
        execution.err().map(|e| Value::string(e.message)).unwrap_or(Value::Null),
    );
    Ok(result)
}

/// An item result as the service returns it: parameterId, status, value and error.
fn item_result_value(i: &ItemResult) -> Value {
    let status = match i.status {
        ItemStatus::Completed => "completed",
        ItemStatus::Unchanged => "unchanged",
        ItemStatus::Failed => "failed",
        ItemStatus::Uncertain => "uncertain",
    };
    Value::Map(indexmap::indexmap! {
        "parameterId".into() => Value::string(&i.parameter_id),
        "status".into() => Value::string(status),
        "value".into() => i.value.as_ref().map(to_value).unwrap_or(Value::Null),
        "error".into() => i.error.as_ref().map(Value::string).unwrap_or(Value::Null),
    })
}

pub fn to_value(v: &FieldValue) -> Value {
    match v {
        FieldValue::Record(v) => Value::Map(v.iter().map(|(k, v)| (k.clone(), to_value(v))).collect()),
        FieldValue::Null => Value::Null,
        FieldValue::Bool(v) => Value::Bool(*v),
        FieldValue::Signed(v) => Value::I64(*v),
        FieldValue::Unsigned(v) => Value::Decimal(Decimal::from(*v)),
        FieldValue::Decimal(v) => Value::Decimal(*v),
        FieldValue::Float(v) => Value::F64(*v),
        FieldValue::Text(v) => Value::string(v),
        FieldValue::Bytes(v) => Value::Bytes(v.clone()),
        FieldValue::Array(v) => Value::List(v.iter().map(to_value).collect()),
    }
}

fn timestamp(time: SystemTime) -> Result<Value, Error> {
    let duration = time.duration_since(UNIX_EPOCH).map_err(error)?;
    Ok(Value::Timestamp(
        jiff::Timestamp::from_nanosecond(duration.as_nanos() as i128).map_err(error)?,
    ))
}

/// The service that stores the samples of a request. A SECA on it runs after each store.
pub const STORE_SAMPLES: &str = "moqui.device.DeviceAcquisitionServices.store#Samples";
/// The service that stores the events of a PLC log subscription.
pub const STORE_PLC_LOG: &str = "moqui.device.DeviceAcquisitionServices.store#LogSamples";
fn optional_text(value: Option<&String>) -> Value {
    value.map(Value::string).unwrap_or(Value::Null)
}

fn sample_to_value(s: &Sample) -> Result<Value, Error> {
    Ok(Value::Map(indexmap::indexmap! {
        "deviceId".into() => Value::string(&s.device_id),
        "parameterId".into() => optional_text(s.parameter_id.as_ref()),
        "value".into() => to_value(&s.value),
        "sourceTime".into() => s.source_time.map(timestamp).transpose()?.unwrap_or(Value::Null),
        "receivedTime".into() => timestamp(s.received_time)?,
        "quality".into() => optional_text(s.quality.as_ref()),
        "sequenceNum".into() => match &s.value {
            FieldValue::Record(event) => event.get("sequenceNum").map(to_value).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        "logValueKind".into() => match s.log_value_kind {
            Some(LogValueKind::Numeric) => Value::string("numeric"),
            Some(LogValueKind::Enumeration) => Value::string("enumeration"),
            Some(LogValueKind::Message) => Value::string("message"),
            None => Value::Null,
        },
    }))
}

/// Calls a store service with the samples (as [`sample_to_value`] gives them) and returns its results; an error the
/// service recorded is an error here.
fn call_store(
    ec: &ExecutionContext,
    service: &str,
    request_name: &str,
    logging: bool,
    samples: Vec<Value>,
    errors: &[String],
) -> Result<Parameters, Error> {
    let mut params = Parameters::new();
    params.insert("requestName".into(), Value::string(request_name));
    params.insert("logging".into(), Value::Bool(logging));
    params.insert("notifyErrors".into(), Value::Bool(false));
    params.insert(
        "acquisitionErrors".into(),
        Value::List(errors.iter().map(Value::string).collect()),
    );
    params.insert("samples".into(), Value::List(samples));
    call_service(ec, service, &params)
}

/// Calls a service; an error it recorded in the message facade is an error here.
fn call_service(ec: &ExecutionContext, service: &str, params: &Parameters) -> Result<Parameters, Error> {
    let mark = ec.message().error_mark();
    let out = moqui_core::service::sync(service, params)?;
    let errors = ec.message().errors_since(mark);
    if !errors.is_empty() {
        return Err(Error::new(errors.join("\n")));
    }
    Ok(out)
}

/// The gateway this process is, from the property `device_gateway_device_id`; empty when not configured.
pub fn gateway_device_id() -> String {
    moqui_lib::env::get_prop_or_env("device_gateway_device_id")
}

/// With a configured gateway, a request runs only for a device in its scope: the DeviceGroups the gateway is a
/// DgmpEdgeGateway member of, and their members, recursively (DeviceAcquisitionServices.get#GatewayScope).
fn check_gateway_scope(ec: &ExecutionContext, request_name: &str, device_id: &str) -> Result<(), Error> {
    let gateway = gateway_device_id();
    if gateway.is_empty() {
        return Ok(());
    }
    let mut params = Parameters::new();
    params.insert("gatewayDeviceId".into(), Value::string(&gateway));
    let scope = call_service(ec, "moqui.device.DeviceAcquisitionServices.get#GatewayScope", &params)?;
    if !matches!(scope.get("ready"), Some(Value::Bool(true))) {
        return Err(Error::new(
            scope
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("The gateway identity is not ready")
                .to_string(),
        ));
    }
    let in_scope = match scope.get("scopedDeviceIds") {
        Some(Value::List(ids)) => ids.iter().any(|id| id.as_str() == Some(device_id)),
        _ => false,
    };
    if !in_scope {
        return Err(Error::new(format!(
            "DeviceRequest {request_name} belongs to device {device_id}, which is not in the scope of gateway {gateway}"
        )));
    }
    Ok(())
}

/// Tells the user that the samples of a request fail to store, or store again
/// (DeviceAcquisitionServices.notify#InboundState).
pub(crate) fn notify_inbound(
    ec: &ExecutionContext,
    batch: &SampleBatch,
    notice: &crate::notifier::Notice,
    error_message: Option<String>,
) -> Result<(), Error> {
    let device_id = batch
        .request
        .as_ref()
        .map(|r| r.device_id.clone())
        .or_else(|| batch.samples.first().map(|s| s.device_id.clone()))
        .ok_or_else(|| Error::new("The batch has no device"))?;
    let (event, duration) = match notice {
        crate::notifier::Notice::Failing(d) => ("inboundError", d),
        crate::notifier::Notice::Recovered(d) => ("inboundRecovered", d),
    };
    // a failed store leaves its errors in the message facade, and a service does not run after errors
    ec.message().clear_errors();
    let mut params = Parameters::new();
    params.insert("requestName".into(), Value::string(&batch.request_name));
    params.insert("deviceId".into(), Value::string(device_id));
    params.insert("eventType".into(), Value::string(event));
    params.insert(
        "errorDurationSeconds".into(),
        Value::I64(i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)),
    );
    params.insert(
        "errorMessage".into(),
        error_message.map(Value::string).unwrap_or(Value::Null),
    );
    call_service(
        ec,
        "moqui.device.DeviceAcquisitionServices.notify#InboundState",
        &params,
    )
    .map(|_| ())
}

/// What the store did with a batch.
pub(crate) enum Stored {
    Done,
    /// Rejected for good: invalid samples or acquisition errors, logged and notified.
    Failed(String),
    /// The database did not answer: the samples from this index on are to be sent again.
    Retry(usize),
}

/// The persistence worker's path: subscription and polling samples go through the store services.
pub(crate) fn store_batch(ec: &ExecutionContext, batch: &SampleBatch) -> Result<Stored, Error> {
    let plc_log = batch.request.as_ref().is_some_and(|r| r.plc_log);
    let samples = batch
        .samples
        .iter()
        .map(sample_to_value)
        .collect::<Result<Vec<_>, _>>()?;
    let out = call_store(
        ec,
        if plc_log { STORE_PLC_LOG } else { STORE_SAMPLES },
        &batch.request_name,
        batch.logging,
        samples,
        &batch.errors,
    )?;
    let count = |name: &str| match out.get(name) {
        Some(Value::I64(n)) => *n,
        Some(Value::I32(n)) => i64::from(*n),
        _ => 0,
    };
    if matches!(out.get("retryable"), Some(Value::Bool(true))) {
        return Ok(Stored::Retry(usize::try_from(count("retryFromIndex")).unwrap_or(0)));
    }
    let failed = count("failedCount");
    Ok(if failed > 0 {
        Stored::Failed(format!("{failed} field samples were not saved"))
    } else if !batch.errors.is_empty() {
        Stored::Failed(batch.errors.join("; "))
    } else {
        Stored::Done
    })
}

/// The facade, runtime and sample queue of the DeviceGateway tool.
pub(crate) fn gateway(ec: &ExecutionContext) -> Result<Arc<crate::tool::Gateway>, Error> {
    ec.factory()
        .get_tool_factory_typed::<DeviceGatewayToolFactory>("DeviceGateway")
        .ok_or_else(|| Error::new("The DeviceGateway tool is not installed"))?
        .gateway()
        .map_err(error)
}
