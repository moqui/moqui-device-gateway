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

//! PLC4J-style Modbus TCP addresses, with zero-based offsets.
use crate::{device_facade::DeviceFacade, model::*};
use async_trait::async_trait;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};
use tokio::{net::TcpStream, sync::Mutex};
use tokio_modbus::{
    client::{tcp, Context, Reader, Writer},
    slave::Slave,
};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Area {
    Coil,
    Discrete,
    Holding,
    Input,
}
#[derive(Clone, Debug)]
pub struct Address {
    pub area: Area,
    pub offset: u16,
    pub ty: PhysicalType,
    pub count: u16,
}
impl Address {
    pub fn parse(text: &str) -> Result<Self, DeviceGatewayError> {
        let p: Vec<_> = text.split(':').collect();
        if p.len() != 3 {
            return Err(DeviceGatewayError::invalid(
                "Modbus address must be area:offset:TYPE[count]",
            ));
        }
        let area = match p[0] {
            "coil" => Area::Coil,
            "discrete-input" => Area::Discrete,
            "holding-register" => Area::Holding,
            "input-register" => Area::Input,
            _ => return Err(DeviceGatewayError::unsupported("Unsupported Modbus area")),
        };
        let offset = p[1]
            .parse::<u16>()
            .map_err(|_| DeviceGatewayError::invalid("Invalid Modbus offset"))?;
        let (ty, count) = if let Some((t, c)) = p[2].split_once('[') {
            (
                t,
                c.strip_suffix(']')
                    .ok_or_else(|| DeviceGatewayError::invalid("Unclosed array"))?
                    .parse::<u16>()
                    .map_err(|_| DeviceGatewayError::invalid("Invalid array count"))?,
            )
        } else {
            (p[2], 1)
        };
        let ty = PhysicalType::parse(ty)?;
        let bits = matches!(area, Area::Coil | Area::Discrete);
        if bits != (ty == PhysicalType::Bool) {
            return Err(DeviceGatewayError::invalid(
                "Coils require BOOL; registers require numeric types",
            ));
        }
        let words = u32::from(count) * u32::from(if bits { 1 } else { ty.registers()? });
        if count == 0 || words > if bits { 2000 } else { 125 } || u32::from(offset) + words > 65536 {
            return Err(DeviceGatewayError::invalid(
                "Modbus request exceeds address or PDU bounds",
            ));
        }
        Ok(Self {
            area,
            offset,
            ty,
            count,
        })
    }
}
#[derive(Default)]
pub struct ModbusDeviceClient {
    sessions: Mutex<HashMap<String, Arc<Mutex<Option<Context>>>>>,
}
fn transport(e: impl std::fmt::Display, writing: bool) -> DeviceGatewayError {
    let e = DeviceGatewayError::transport(format!("Modbus transport failed: {e}"));
    if writing {
        e.uncertain()
    } else {
        e
    }
}
fn reorder(w: &mut [u16], bytes: bool, words: bool) {
    if bytes {
        for w in w.iter_mut() {
            *w = w.swap_bytes();
        }
    }
    if words {
        w.reverse();
    }
}
pub fn decode_words(w: &[u16], ty: PhysicalType, bytes: bool, words: bool) -> Result<FieldValue, DeviceGatewayError> {
    if w.len() != ty.registers()? as usize {
        return Err(DeviceGatewayError::invalid("Unexpected register count"));
    }
    let mut w = w.to_vec();
    reorder(&mut w, bytes, words);
    let n = w.iter().fold(0u64, |n, w| (n << 16) | u64::from(*w));
    Ok(match ty {
        PhysicalType::U8 if n <= 255 => FieldValue::Unsigned(n),
        PhysicalType::U16 | PhysicalType::U32 | PhysicalType::U64 => FieldValue::Unsigned(n),
        PhysicalType::I8 if i8::try_from(n as u16 as i16).is_ok() => FieldValue::Signed(n as u16 as i16 as i64),
        PhysicalType::I16 => FieldValue::Signed(n as u16 as i16 as i64),
        PhysicalType::I32 => FieldValue::Signed(n as u32 as i32 as i64),
        PhysicalType::I64 => FieldValue::Signed(n as i64),
        PhysicalType::F32 => FieldValue::Float(f32::from_bits(n as u32) as f64),
        PhysicalType::F64 => FieldValue::Float(f64::from_bits(n)),
        _ => return Err(DeviceGatewayError::invalid("Register value outside type range")),
    })
}
pub fn encode_words(
    v: &FieldValue,
    ty: PhysicalType,
    bytes: bool,
    words: bool,
) -> Result<Vec<u16>, DeviceGatewayError> {
    let v = crate::codec::ValueCodec::default().encode(v, ParameterKind::Numeric, ty)?;
    let n = match v {
        FieldValue::Unsigned(n) => n,
        FieldValue::Signed(n) => n as u64,
        FieldValue::Float(n) => {
            if ty == PhysicalType::F32 {
                (n as f32).to_bits() as u64
            } else {
                n.to_bits()
            }
        }
        _ => return Err(DeviceGatewayError::invalid("Register value must be numeric")),
    };
    let count = ty.registers()? as usize;
    let mut w = (0..count).rev().map(|i| (n >> (i * 16)) as u16).collect::<Vec<_>>();
    reorder(&mut w, bytes, words);
    Ok(w)
}
#[async_trait]
impl DeviceFacade for ModbusDeviceClient {
    fn capabilities(&self) -> Capabilities {
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
            if !matches!(k.as_str(), "unit-identifier" | "byte-order" | "word-order") {
                return Err(DeviceGatewayError::unsupported(format!("Unknown Modbus option: {k}")));
            }
        }
        let option = |k: &str| r.connection.options.get(k).map(String::as_str);
        let unit = option("unit-identifier")
            .unwrap_or("1")
            .parse::<u8>()
            .map_err(|_| DeviceGatewayError::invalid("Invalid unit identifier"))?;
        if unit == 0 {
            return Err(DeviceGatewayError::unsupported(
                "Unacknowledged broadcast writes are not supported",
            ));
        }
        let swap = |k| match option(k).unwrap_or("big-endian") {
            "big-endian" => Ok(false),
            "little-endian" => Ok(true),
            _ => Err(DeviceGatewayError::invalid(
                "Byte/word order must be big-endian or little-endian",
            )),
        };
        let bytes = swap("byte-order")?;
        let words = swap("word-order")?;
        let mut plans = Vec::new();
        for i in &r.items {
            let a = Address::parse(&i.address)?;
            if i.bulk_index.is_some_and(|index| index >= usize::from(a.count)) {
                return Err(DeviceGatewayError::invalid("Bulk index exceeds configured array count"));
            }
            if a.ty != i.physical_type {
                return Err(DeviceGatewayError::invalid("Address and item types disagree"));
            }
            if writing && matches!(a.area, Area::Input | Area::Discrete) {
                return Err(DeviceGatewayError::invalid("Cannot write a read-only Modbus area"));
            }
            if writing && a.count > 1 && i.bulk_index.is_none() {
                return Err(DeviceGatewayError::invalid(
                    "Bulk writes require one indexed item per array value",
                ));
            }
            let v = if writing {
                Some(
                    input
                        .values
                        .get(&i.parameter_id)
                        .ok_or_else(|| DeviceGatewayError::invalid("Missing write value"))?,
                )
            } else {
                None
            };
            let w = if writing && a.ty != PhysicalType::Bool {
                encode_words(v.unwrap(), a.ty, bytes, words)?
            } else {
                Vec::new()
            };
            if writing && a.ty == PhysicalType::Bool && !matches!(v, Some(FieldValue::Bool(_))) {
                return Err(DeviceGatewayError::invalid("Coil writes require booleans"));
            }
            plans.push((i, a, v, w));
        }
        if plans.is_empty() {
            return Err(DeviceGatewayError::invalid("Modbus request has no items"));
        }
        // A bulk PDU is assembled and validated in full before opening a connection or sending any command.
        let mut bulk_words = HashMap::new();
        let mut bulk_coils = HashMap::new();
        if writing {
            for (item, address, _, _) in &plans {
                if address.count == 1
                    || bulk_words.contains_key(&item.address)
                    || bulk_coils.contains_key(&item.address)
                {
                    continue;
                }
                let mut indices = vec![None; address.count as usize];
                for (other, _, value, words) in &plans {
                    if other.address == item.address {
                        let index = other
                            .bulk_index
                            .ok_or_else(|| DeviceGatewayError::invalid("Missing bulk index"))?;
                        if indices[index].replace((value, words)).is_some() {
                            return Err(DeviceGatewayError::invalid("Duplicate bulk write index"));
                        }
                    }
                }
                if indices.iter().any(Option::is_none) {
                    return Err(DeviceGatewayError::invalid(
                        "Bulk write must provide every configured array element",
                    ));
                }
                if address.area == Area::Coil {
                    if address.count > 1968 {
                        return Err(DeviceGatewayError::invalid("Modbus coil write exceeds PDU bounds"));
                    }
                    let values = indices
                        .into_iter()
                        .map(|entry| match entry {
                            Some((Some(FieldValue::Bool(value)), _)) => Ok(*value),
                            _ => Err(DeviceGatewayError::invalid("Bulk coil write requires booleans")),
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    bulk_coils.insert(item.address.clone(), values);
                } else {
                    let values = indices
                        .into_iter()
                        .flat_map(|entry| entry.into_iter().flat_map(|(_, words)| words.iter().copied()))
                        .collect::<Vec<_>>();
                    if values.len() > 123 {
                        return Err(DeviceGatewayError::invalid("Modbus register write exceeds PDU bounds"));
                    }
                    bulk_words.insert(item.address.clone(), values);
                }
            }
        }
        let slot = self
            .sessions
            .lock()
            .await
            .entry(r.connection.session_key())
            .or_default()
            .clone();
        let mut slot = tokio::time::timeout_at(deadline.into(), slot.lock())
            .await
            .map_err(|_| DeviceGatewayError::timeout(false))?;
        // Cancellation drops the taken context, preventing reuse of a stale response.
        let mut ctx = if let Some(ctx) = slot.take() {
            ctx
        } else {
            let ep = r
                .connection
                .endpoint
                .strip_prefix("modbus-tcp://")
                .unwrap_or(&r.connection.endpoint);
            let stream = tokio::time::timeout_at(deadline.into(), TcpStream::connect(ep))
                .await
                .map_err(|_| DeviceGatewayError::timeout(false))?
                .map_err(|e| transport(e, false))?;
            tcp::attach_slave(stream, Slave(unit))
        };
        let mut read_cache: HashMap<String, Vec<FieldValue>> = HashMap::new();
        let mut written_bulk = HashSet::new();
        for (i, a, v, w) in plans {
            let op = async {
                let exception = |e| DeviceGatewayError::transport(format!("Modbus exception: {e}"));
                if writing {
                    if a.count > 1 {
                        if !written_bulk.contains(&i.address) {
                            if let Some(values) = bulk_coils.get(&i.address) {
                                ctx.write_multiple_coils(a.offset, values)
                                    .await
                                    .map_err(|e| transport(e, true))?
                                    .map_err(exception)?;
                            } else if let Some(values) = bulk_words.get(&i.address) {
                                ctx.write_multiple_registers(a.offset, values)
                                    .await
                                    .map_err(|e| transport(e, true))?
                                    .map_err(exception)?;
                            }
                            written_bulk.insert(i.address.clone());
                        }
                        return Ok(v
                            .ok_or_else(|| DeviceGatewayError::invalid("Missing bulk value"))?
                            .clone());
                    }
                    match a.area {
                        Area::Coil => {
                            let FieldValue::Bool(v) = v.unwrap() else {
                                unreachable!()
                            };
                            ctx.write_single_coil(a.offset, *v)
                                .await
                                .map_err(|e| transport(e, true))?
                                .map_err(exception)?;
                        }
                        Area::Holding => {
                            if w.len() == 1 {
                                ctx.write_single_register(a.offset, w[0])
                                    .await
                                    .map_err(|e| transport(e, true))?
                                    .map_err(exception)?;
                            } else {
                                ctx.write_multiple_registers(a.offset, &w)
                                    .await
                                    .map_err(|e| transport(e, true))?
                                    .map_err(exception)?;
                            }
                        }
                        _ => unreachable!(),
                    }
                    Ok(v.unwrap().clone())
                } else {
                    let values = if let Some(values) = read_cache.get(&i.address) {
                        values.clone()
                    } else {
                        let values = match a.area {
                            Area::Coil | Area::Discrete => {
                                let v = if a.area == Area::Coil {
                                    ctx.read_coils(a.offset, a.count).await
                                } else {
                                    ctx.read_discrete_inputs(a.offset, a.count).await
                                }
                                .map_err(|e| transport(e, false))?
                                .map_err(exception)?;
                                v.into_iter().map(FieldValue::Bool).collect::<Vec<_>>()
                            }
                            Area::Holding | Area::Input => {
                                let width = a.ty.registers()?;
                                let v = if a.area == Area::Holding {
                                    ctx.read_holding_registers(a.offset, a.count * width).await
                                } else {
                                    ctx.read_input_registers(a.offset, a.count * width).await
                                }
                                .map_err(|e| transport(e, false))?
                                .map_err(exception)?;
                                v.chunks(width as usize)
                                    .map(|w| decode_words(w, a.ty, bytes, words))
                                    .collect::<Result<Vec<_>, _>>()?
                            }
                        };
                        read_cache.insert(i.address.clone(), values.clone());
                        values
                    };
                    if let Some(index) = i.bulk_index {
                        values
                            .get(index)
                            .cloned()
                            .ok_or_else(|| DeviceGatewayError::invalid("Bulk index exceeds response length"))
                    } else if values.len() == 1 {
                        Ok(values.into_iter().next().unwrap())
                    } else {
                        Ok(FieldValue::Array(values))
                    }
                }
            };
            match tokio::time::timeout_at(deadline.into(), op).await {
                Ok(Ok(v)) => out.items.push(ItemResult::value(&i.parameter_id, v)),
                result => {
                    let e = match result {
                        Ok(Err(e)) => e,
                        Err(_) => DeviceGatewayError::timeout(writing),
                        _ => unreachable!(),
                    };
                    out.items.push(ItemResult::error(&i.parameter_id, e.clone()));
                    return Err(e);
                }
            }
        }
        *slot = Some(ctx);
        Ok(())
    }
    async fn close(&self) {
        self.sessions.lock().await.clear();
    }
}
