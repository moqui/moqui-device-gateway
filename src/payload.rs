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

//! The MQTT payload format: JSON values that keep integer and decimal precision across the transport, the item
//! envelopes of moqui-plc4j and the Camel gateway, and the log events of moqui-plc.

use crate::model::*;
use serde_json::{Map, Number, Value};
use std::str::FromStr;

pub fn to_json(v: &FieldValue) -> Result<Value, DeviceGatewayError> {
    Ok(match v {
        FieldValue::Record(v) => Value::Object(
            v.iter()
                .map(|(k, v)| Ok((k.clone(), to_json(v)?)))
                .collect::<Result<_, DeviceGatewayError>>()?,
        ),
        FieldValue::Null => Value::Null,
        FieldValue::Bool(v) => Value::Bool(*v),
        FieldValue::Signed(v) => Value::Number((*v).into()),
        FieldValue::Unsigned(v) => Value::Number((*v).into()),
        FieldValue::Decimal(v) => Value::Number(
            Number::from_str(&v.to_string()).map_err(|_| DeviceGatewayError::invalid("Invalid JSON decimal"))?,
        ),
        FieldValue::Float(v) => {
            Value::Number(Number::from_f64(*v).ok_or_else(|| DeviceGatewayError::invalid("Non-finite JSON number"))?)
        }
        FieldValue::Text(v) => Value::String(v.clone()),
        FieldValue::Bytes(v) => Value::Array(v.iter().map(|n| Value::Number((*n).into())).collect()),
        FieldValue::Array(v) => Value::Array(v.iter().map(to_json).collect::<Result<_, _>>()?),
    })
}

pub fn from_json(v: &Value) -> Result<FieldValue, DeviceGatewayError> {
    Ok(match v {
        Value::Null => FieldValue::Null,
        Value::Bool(v) => FieldValue::Bool(*v),
        Value::String(v) => FieldValue::Text(v.clone()),
        Value::Number(v) => {
            if let Some(n) = v.as_i64() {
                FieldValue::Signed(n)
            } else if let Some(n) = v.as_u64() {
                FieldValue::Unsigned(n)
            } else {
                FieldValue::Decimal(
                    v.to_string()
                        .parse()
                        .map_err(|_| DeviceGatewayError::invalid("JSON number exceeds decimal range"))?,
                )
            }
        }
        Value::Array(v) => FieldValue::Array(v.iter().map(from_json).collect::<Result<_, _>>()?),
        Value::Object(v) => FieldValue::Record(
            v.iter()
                .map(|(k, v)| Ok((k.clone(), from_json(v)?)))
                .collect::<Result<_, DeviceGatewayError>>()?,
        ),
    })
}

/// One JSON object with a value per item, keyed by item name.
pub fn write_payload(r: &PreparedDeviceRequest, input: &RequestInput) -> Result<Vec<u8>, DeviceGatewayError> {
    let mut p = Map::new();
    for i in &r.items {
        if p.contains_key(&i.name) {
            return Err(DeviceGatewayError::invalid("Duplicate MQTT item name"));
        }
        p.insert(
            i.name.clone(),
            to_json(
                input
                    .values
                    .get(&i.parameter_id)
                    .ok_or_else(|| DeviceGatewayError::invalid("Missing MQTT value"))?,
            )?,
        );
    }
    serde_json::to_vec(&p).map_err(|e| DeviceGatewayError::invalid(e.to_string()))
}

/// The values of the items of a request in one message.
pub fn read_payload(r: &PreparedDeviceRequest, bytes: &[u8]) -> Result<Vec<ItemResult>, DeviceGatewayError> {
    read_items(&r.items.iter().collect::<Vec<_>>(), bytes)
}

/// The values of some items in one message: a scalar, an object with the item name or the envelope field
/// (`numericValue`, `parameterEnumId`, `symbolicValue`), or an array with one such row per item, in order. A row whose
/// `parameterId` names another parameter fails the item.
pub fn read_items(items: &[&Item], bytes: &[u8]) -> Result<Vec<ItemResult>, DeviceGatewayError> {
    let p: Value =
        serde_json::from_slice(bytes).map_err(|_| DeviceGatewayError::invalid("Invalid MQTT JSON payload"))?;
    Ok(items
        .iter()
        .enumerate()
        .map(|(index, i)| {
            let row = if let Value::Array(rows) = &p {
                rows.get(index)
            } else {
                Some(&p)
            };
            let value = row.and_then(|row| {
                if row
                    .get("parameterId")
                    .and_then(Value::as_str)
                    .is_some_and(|id| id != i.parameter_id)
                {
                    return None;
                }
                if !row.is_object() {
                    return Some(row);
                }
                row.get(&i.name).or_else(|| {
                    row.get(match i.kind {
                        ParameterKind::Numeric => "numericValue",
                        ParameterKind::Bits => "parameterEnumId",
                        _ => "symbolicValue",
                    })
                })
            });
            match value {
                Some(v) => match from_json(v) {
                    Ok(v) => ItemResult::value(&i.parameter_id, v),
                    Err(e) => ItemResult::error(&i.parameter_id, e),
                },
                None => ItemResult::error(
                    &i.parameter_id,
                    DeviceGatewayError::invalid("MQTT payload has no matching parameter identity or configured item"),
                ),
            }
        })
        .collect())
}

/// The message of one item: the envelope of moqui-plc4j (`parameterId` and the value field of its kind), plus the value
/// under the item name.
pub fn write_item_payload(i: &Item, input: &RequestInput) -> Result<Vec<u8>, DeviceGatewayError> {
    if matches!(
        i.name.as_str(),
        "parameterId" | "numericValue" | "symbolicValue" | "parameterEnumId"
    ) {
        return Err(DeviceGatewayError::invalid("Reserved MQTT item name"));
    }
    let value = input
        .values
        .get(&i.parameter_id)
        .ok_or_else(|| DeviceGatewayError::invalid("Missing MQTT write value"))?;
    let mut p = Map::new();
    p.insert("parameterId".into(), Value::String(i.parameter_id.clone()));
    let v = to_json(value)?;
    let (field, envelope, named) = match (i.kind, value) {
        (ParameterKind::Numeric, _) => ("numericValue", v.clone(), v),
        (ParameterKind::Indicator, FieldValue::Bool(b)) => {
            ("symbolicValue", Value::String(if *b { "Y" } else { "N" }.into()), v)
        }
        (ParameterKind::Bits, FieldValue::Unsigned(n)) => {
            let width = match i.physical_type {
                PhysicalType::U8 => 8,
                PhysicalType::U16 => 16,
                PhysicalType::U32 => 32,
                PhysicalType::U64 => 64,
                _ => {
                    return Err(DeviceGatewayError::invalid(
                        "MQTT bitset requires an unsigned physical type",
                    ));
                }
            };
            let value = Value::String(format!("{n:0width$b}"));
            ("parameterEnumId", value.clone(), value)
        }
        (ParameterKind::Text, _) => ("symbolicValue", v.clone(), v),
        _ => return Err(DeviceGatewayError::invalid("Invalid MQTT indicator/bitset value")),
    };
    p.insert(field.into(), envelope);
    if !i.name.is_empty() {
        p.insert(i.name.clone(), named);
    }
    serde_json::to_vec(&p).map_err(|e| DeviceGatewayError::invalid(e.to_string()))
}

/// The events of a moqui-plc log message: one event, or a map of events. The facade attaches the request to the batch.
pub fn plc_log_batch(r: &PreparedDeviceRequest, bytes: &[u8]) -> Result<SampleBatch, DeviceGatewayError> {
    let p: Value = serde_json::from_slice(bytes).map_err(|_| DeviceGatewayError::invalid("Invalid PLC log JSON"))?;
    let events = if p.get("loggerName").is_some() {
        vec![&p]
    } else {
        p.as_object()
            .ok_or_else(|| DeviceGatewayError::invalid("PLC log must be an event or map of events"))?
            .values()
            .collect()
    };
    let mut samples = Vec::new();
    for event in events {
        let device = event
            .get("loggerName")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| DeviceGatewayError::invalid("PLC event requires loggerName"))?;
        // Keep the sender's exact identity. The host validates it against the request's approved device hierarchy.
        let parameter = event
            .get("source")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(String::from);
        let value = if parameter.is_none() {
            event
        } else {
            match event.get("type").and_then(Value::as_i64) {
                Some(1) => event.get("numericValue"),
                Some(2) => event.get("enumValue"),
                _ => event.get("message"),
            }
            .ok_or_else(|| DeviceGatewayError::invalid("PLC event has no value for its type"))?
        };
        let source_time = event
            .get("logEventDate")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(|s| {
                if let Ok(t) = chrono::DateTime::parse_from_rfc3339(s) {
                    return Ok(std::time::SystemTime::from(t));
                }
                chrono::NaiveDateTime::parse_from_str(s, "DT#%Y-%m-%d-%H:%M:%S")
                    .map(|t| std::time::SystemTime::from(t.and_utc()))
                    .map_err(|_| {
                        DeviceGatewayError::invalid("Invalid PLC log timestamp; DT# dates are interpreted as UTC")
                    })
            })
            .transpose()?;
        samples.push(Sample {
            device_id: device.into(),
            log_value_kind: parameter
                .as_ref()
                .map(|_| match event.get("type").and_then(Value::as_i64) {
                    Some(1) => LogValueKind::Numeric,
                    Some(2) => LogValueKind::Enumeration,
                    _ => LogValueKind::Message,
                }),
            parameter_id: parameter,
            value: from_json(value)?,
            source_time,
            received_time: std::time::SystemTime::now(),
            quality: None,
        });
    }
    Ok(SampleBatch {
        errors: Vec::new(),
        request_name: r.name.clone(),
        logging: true,
        request: None,
        samples,
    })
}
