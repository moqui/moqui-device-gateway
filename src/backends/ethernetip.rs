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

//! EtherNet/IP with rust-ethernet-ip: the symbolic tags of Allen-Bradley Logix controllers (CompactLogix,
//! ControlLogix), as the PLC4X `logix` driver. The address of an item is the tag name, as the controller knows it:
//! `Motor_Speed`, `Line[3]`, `Recipe.Temperature`, `Program:Main.Counter`. The physical type of the item gives the
//! CIP type of a write; a read returns the type the controller sends.

use crate::{device_facade::DeviceFacade, model::*};
use async_trait::async_trait;
use rust_ethernet_ip::{EipClient, PlcValue, RoutePath};
use std::{collections::HashMap, sync::Arc, time::Instant};
use tokio::sync::Mutex;

#[derive(Default)]
pub struct EthernetIpDeviceClient {
    sessions: Mutex<HashMap<String, Arc<Mutex<Option<EipClient>>>>>,
}

fn transport(e: impl std::fmt::Display, writing: bool) -> DeviceGatewayError {
    let e = DeviceGatewayError::transport(format!("EtherNet/IP failed: {e}"));
    if writing {
        e.uncertain()
    } else {
        e
    }
}

/// `host:port` of an endpoint `logix://host[:port]` or `eip://host[:port]`; the EtherNet/IP port is 44818.
pub fn socket_address(endpoint: &str) -> Result<String, DeviceGatewayError> {
    let host = endpoint
        .strip_prefix("logix://")
        .or_else(|| endpoint.strip_prefix("eip://"))
        .unwrap_or(endpoint)
        .trim_end_matches('/');
    if host.is_empty() || host.contains('/') {
        return Err(DeviceGatewayError::invalid(
            "EtherNet/IP endpoint must be logix://host[:port]",
        ));
    }
    Ok(
        if host
            .rsplit_once(':')
            .is_some_and(|(_, port)| port.parse::<u16>().is_ok())
        {
            host.to_string()
        } else {
            format!("{host}:44818")
        },
    )
}

/// The CIP value of a physical value, by the physical type of the item.
pub fn to_plc(value: &FieldValue, ty: PhysicalType) -> Result<PlcValue, DeviceGatewayError> {
    let out_of_range = || DeviceGatewayError::invalid("Value outside the CIP type range");
    let signed = |v: &FieldValue| match v {
        FieldValue::Signed(n) => Ok(*n),
        FieldValue::Unsigned(n) => i64::try_from(*n).map_err(|_| out_of_range()),
        _ => Err(DeviceGatewayError::invalid("An integer tag needs an integer value")),
    };
    let unsigned = |v: &FieldValue| match v {
        FieldValue::Unsigned(n) => Ok(*n),
        FieldValue::Signed(n) => u64::try_from(*n).map_err(|_| out_of_range()),
        _ => Err(DeviceGatewayError::invalid("An integer tag needs an integer value")),
    };
    let float = |v: &FieldValue| match v {
        FieldValue::Float(n) => Ok(*n),
        FieldValue::Signed(n) => Ok(*n as f64),
        FieldValue::Unsigned(n) => Ok(*n as f64),
        _ => Err(DeviceGatewayError::invalid("A REAL tag needs a number")),
    };
    Ok(match ty {
        PhysicalType::Bool => match value {
            FieldValue::Bool(b) => PlcValue::Bool(*b),
            _ => return Err(DeviceGatewayError::invalid("A BOOL tag needs a boolean")),
        },
        PhysicalType::I8 => PlcValue::Sint(i8::try_from(signed(value)?).map_err(|_| out_of_range())?),
        PhysicalType::I16 => PlcValue::Int(i16::try_from(signed(value)?).map_err(|_| out_of_range())?),
        PhysicalType::I32 => PlcValue::Dint(i32::try_from(signed(value)?).map_err(|_| out_of_range())?),
        PhysicalType::I64 => PlcValue::Lint(signed(value)?),
        PhysicalType::U8 => PlcValue::Usint(u8::try_from(unsigned(value)?).map_err(|_| out_of_range())?),
        PhysicalType::U16 => PlcValue::Uint(u16::try_from(unsigned(value)?).map_err(|_| out_of_range())?),
        PhysicalType::U32 => PlcValue::Udint(u32::try_from(unsigned(value)?).map_err(|_| out_of_range())?),
        PhysicalType::U64 => PlcValue::Ulint(unsigned(value)?),
        PhysicalType::F32 => PlcValue::Real(float(value)? as f32),
        PhysicalType::F64 => PlcValue::Lreal(float(value)?),
        PhysicalType::Text => match value {
            FieldValue::Text(t) => PlcValue::String(t.clone()),
            _ => return Err(DeviceGatewayError::invalid("A STRING tag needs text")),
        },
        PhysicalType::Bytes => {
            return Err(DeviceGatewayError::unsupported(
                "EtherNet/IP byte strings and UDTs are not implemented",
            ))
        }
    })
}

/// The physical value of a CIP value.
pub fn from_plc(value: PlcValue) -> Result<FieldValue, DeviceGatewayError> {
    Ok(match value {
        PlcValue::Bool(b) => FieldValue::Bool(b),
        PlcValue::Sint(n) => FieldValue::Signed(n.into()),
        PlcValue::Int(n) => FieldValue::Signed(n.into()),
        PlcValue::Dint(n) => FieldValue::Signed(n.into()),
        PlcValue::Lint(n) => FieldValue::Signed(n),
        PlcValue::Usint(n) => FieldValue::Unsigned(n.into()),
        PlcValue::Uint(n) => FieldValue::Unsigned(n.into()),
        PlcValue::Udint(n) => FieldValue::Unsigned(n.into()),
        PlcValue::Ulint(n) => FieldValue::Unsigned(n),
        PlcValue::Real(n) => FieldValue::Float(n.into()),
        PlcValue::Lreal(n) => FieldValue::Float(n),
        PlcValue::String(t) => FieldValue::Text(t),
        PlcValue::Udt(_) => {
            return Err(DeviceGatewayError::unsupported(
                "EtherNet/IP UDT values are not implemented: address a member of the structure",
            ))
        }
    })
}

#[async_trait]
impl DeviceFacade for EthernetIpDeviceClient {
    fn capabilities(&self) -> Capabilities {
        // CIP explicit messaging has no change notification: poll a DrtRead instead.
        Capabilities {
            read: true,
            write: true,
            subscribe: false,
        }
    }

    async fn execute(
        &self,
        r: &PreparedDeviceRequest,
        input: &RequestInput,
        out: &mut RequestOutput,
        deadline: Instant,
    ) -> Result<(), DeviceGatewayError> {
        let writing = r.operation == Operation::Write;
        for k in r.connection.options.keys() {
            if k != "slot" {
                return Err(DeviceGatewayError::unsupported(format!(
                    "Unknown EtherNet/IP option: {k}"
                )));
            }
        }
        let slot = r
            .connection
            .options
            .get("slot")
            .map(|s| s.parse::<u8>())
            .transpose()
            .map_err(|_| DeviceGatewayError::invalid("slot must be a backplane slot 0..255"))?;
        if r.items.is_empty() {
            return Err(DeviceGatewayError::invalid("EtherNet/IP request has no items"));
        }
        // Every value is converted before connecting, so a bad value sends nothing.
        let mut plans = Vec::new();
        for item in &r.items {
            if item.address.trim().is_empty() {
                return Err(DeviceGatewayError::invalid("An EtherNet/IP item needs a tag name"));
            }
            let value = if writing {
                let value = input
                    .values
                    .get(&item.parameter_id)
                    .ok_or_else(|| DeviceGatewayError::invalid("Missing write value"))?;
                Some(to_plc(value, item.physical_type)?)
            } else {
                None
            };
            plans.push((item, value));
        }
        let address = socket_address(&r.connection.endpoint)?;
        let session = self
            .sessions
            .lock()
            .await
            .entry(r.connection.session_key())
            .or_default()
            .clone();
        let mut session = tokio::time::timeout_at(deadline.into(), session.lock())
            .await
            .map_err(|_| DeviceGatewayError::timeout(false))?;
        // A session that fails or is cancelled is dropped, never reused.
        let mut client = match session.take() {
            Some(client) => client,
            None => {
                let mut client = tokio::time::timeout_at(deadline.into(), EipClient::connect(&address))
                    .await
                    .map_err(|_| DeviceGatewayError::timeout(false))?
                    .map_err(|e| transport(e, false))?;
                if let Some(slot) = slot {
                    client.set_route_path(RoutePath::new().add_slot(slot));
                }
                client
            }
        };
        for (item, value) in plans {
            let tag = item.address.trim();
            let result = match value {
                Some(value) => {
                    let sent = FieldValue::clone(&input.values[&item.parameter_id]);
                    tokio::time::timeout_at(deadline.into(), client.write_tag(tag, value))
                        .await
                        .map_err(|_| DeviceGatewayError::timeout(true))
                        .and_then(|r| r.map_err(|e| transport(e, true)))
                        .map(|()| sent)
                }
                None => tokio::time::timeout_at(deadline.into(), client.read_tag(tag))
                    .await
                    .map_err(|_| DeviceGatewayError::timeout(false))
                    .and_then(|r| r.map_err(|e| transport(e, false)))
                    .and_then(from_plc),
            };
            match result {
                Ok(v) => out.items.push(ItemResult::value(&item.parameter_id, v)),
                Err(e) => {
                    out.items.push(ItemResult::error(&item.parameter_id, e.clone()));
                    return Err(e);
                }
            }
        }
        *session = Some(client);
        Ok(())
    }

    async fn close(&self) {
        let sessions = std::mem::take(&mut *self.sessions.lock().await);
        for session in sessions.into_values() {
            if let Some(mut client) = session.lock().await.take() {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(2), client.unregister_session()).await;
            }
        }
    }
}
