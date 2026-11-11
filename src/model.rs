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

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt,
    time::{Duration, Instant, SystemTime},
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "camelCase")]
pub enum FieldValue {
    Null,
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    Decimal(Decimal),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    Array(Vec<FieldValue>),
    Record(BTreeMap<String, FieldValue>),
}
impl FieldValue {
    pub fn decimal(&self) -> Result<Decimal, DeviceGatewayError> {
        match self {
            Self::Signed(v) => Ok(Decimal::from(*v)),
            Self::Unsigned(v) => Ok(Decimal::from(*v)),
            Self::Decimal(v) => Ok(*v),
            Self::Float(v) if v.is_finite() => v
                .to_string()
                .parse()
                .map_err(|_| DeviceGatewayError::invalid("Numeric value is out of decimal range")),
            _ => Err(DeviceGatewayError::invalid("A finite numeric value is required")),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PhysicalType {
    Bool,
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
    Text,
    Bytes,
}
impl PhysicalType {
    pub fn registers(self) -> Result<u16, DeviceGatewayError> {
        match self {
            Self::U8 | Self::I8 | Self::U16 | Self::I16 => Ok(1),
            Self::U32 | Self::I32 | Self::F32 => Ok(2),
            Self::U64 | Self::I64 | Self::F64 => Ok(4),
            _ => Err(DeviceGatewayError::unsupported("Type is not a Modbus register type")),
        }
    }
    pub fn parse(value: &str) -> Result<Self, DeviceGatewayError> {
        Ok(match value.to_ascii_uppercase().as_str() {
            "BOOL" | "DRITBOOL" => Self::Bool,
            "BYTE" | "USINT" | "DRITUSINT" | "DRITBYTE" => Self::U8,
            "WORD" | "UINT" | "DRITUINT" | "DRITWORD" => Self::U16,
            "DWORD" | "UDINT" | "DRITUDINT" | "DRITDWORD" => Self::U32,
            "LWORD" | "ULINT" | "DRITULINT" | "DRITLWORD" => Self::U64,
            "SINT" | "DRITSINT" => Self::I8,
            "INT" | "DRITINT" => Self::I16,
            "DINT" | "DRITDINT" => Self::I32,
            "LINT" | "DRITLINT" => Self::I64,
            "REAL" | "DRITREAL" => Self::F32,
            "LREAL" | "DRITLREAL" => Self::F64,
            "STRING" | "DRITSTRING" | "TEXT" => Self::Text,
            "BYTES" => Self::Bytes,
            _ => return Err(DeviceGatewayError::unsupported("Unknown physical item type")),
        })
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    Read,
    Write,
    Subscribe,
    Poll,
    Unsubscribe,
}
impl Operation {
    /// The operation of a request type. A `DrtRead` is a single read; the host makes it a [`Self::Poll`] when the
    /// request has a polling interval. The three subscription types are those of PLC4X.
    pub fn from_enum(value: &str) -> Result<Self, DeviceGatewayError> {
        Ok(match value {
            "DrtRead" => Self::Read,
            "DrtWrite" | "DrtConfigWrite" => Self::Write,
            "DrtSubscribe" | "DrtEvent" | "DrtStateChange" | "DrtCyclic" => Self::Subscribe,
            "DrtUnsubscribe" => Self::Unsubscribe,
            _ => return Err(DeviceGatewayError::unsupported("Unsupported DeviceRequest type")),
        })
    }
}
/// What makes a subscription send a value, as the PLC4X subscription types: `DrtStateChange` (and the generic
/// `DrtSubscribe`), `DrtCyclic` and `DrtEvent`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SubscriptionType {
    /// A value that changes, which the device samples at its own rate.
    #[default]
    ChangeOfState,
    /// A value sampled at the request's polling interval.
    Cyclic,
    /// An event or a message.
    Event,
}
impl SubscriptionType {
    pub fn from_enum(value: &str) -> Self {
        match value {
            "DrtCyclic" => Self::Cyclic,
            "DrtEvent" => Self::Event,
            _ => Self::ChangeOfState,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Protocol {
    ModbusTcp,
    Mqtt,
    OpcUa,
    /// EtherNet/IP with the CIP services of Logix controllers: symbolic tags (PLC4X `logix`, `eip`).
    EthernetIp,
    /// An EtherCAT segment on a raw Ethernet interface, this host as its MainDevice.
    EtherCat,
}
#[derive(Clone, Default)]
pub struct Credentials {
    pub username: Option<String>,
    pub password: Option<String>,
}
impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("username", &self.username.as_ref().map(|_| "<redacted>"))
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}
#[derive(Clone)]
pub struct Connection {
    pub name: String,
    pub protocol: Protocol,
    pub endpoint: String,
    pub options: BTreeMap<String, String>,
    pub credentials: Credentials,
}
impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("name", &self.name)
            .field("protocol", &self.protocol)
            .field("endpoint", &"<configured>")
            .field("options", &"<redacted>")
            .finish()
    }
}
impl Connection {
    /// The key includes credentials but is never exposed through Debug or diagnostics.
    #[cfg(any(feature = "modbus", feature = "opcua", feature = "ethernetip"))]
    pub(crate) fn session_key(&self) -> String {
        format!(
            "{:?}\0{}\0{:?}\0{:?}\0{:?}",
            self.protocol, self.endpoint, self.options, self.credentials.username, self.credentials.password
        )
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParameterKind {
    Numeric,
    Indicator,
    Bits,
    Text,
}
#[derive(Clone, Debug)]
pub struct Item {
    pub parameter_id: String,
    pub name: String,
    pub address: String,
    pub physical_type: PhysicalType,
    pub kind: ParameterKind,
    pub codec: crate::codec::ValueCodec,
    pub bulk_index: Option<usize>,
}
#[derive(Clone, Debug)]
pub struct PreparedDeviceRequest {
    pub name: String,
    pub device_id: String,
    pub parent_request: Option<String>,
    pub operation: Operation,
    /// For [`Operation::Subscribe`].
    pub subscription_type: SubscriptionType,
    pub connection: Connection,
    pub query: Option<String>,
    pub items: Vec<Item>,
    pub timeout: Duration,
    pub polling_interval: Duration,
    pub logging: bool,
    pub plc_log: bool,
    pub only_changed: bool,
}
impl PreparedDeviceRequest {
    pub fn validate(&self) -> Result<(), DeviceGatewayError> {
        if self.name.is_empty() || self.device_id.is_empty() || self.connection.endpoint.is_empty() {
            return Err(DeviceGatewayError::invalid("Request, device and endpoint are required"));
        }
        if self.timeout.is_zero()
            || self.timeout > Duration::from_secs(86400)
            || self.polling_interval.is_zero()
            || self.polling_interval > Duration::from_secs(86400)
        {
            return Err(DeviceGatewayError::invalid(
                "Timeout and polling interval must be positive milliseconds",
            ));
        }
        let mut ids = std::collections::HashSet::new();
        for item in &self.items {
            if item.parameter_id.is_empty() || !ids.insert(&item.parameter_id) {
                return Err(DeviceGatewayError::invalid(
                    "Item parameter identities must be non-empty and unique",
                ));
            }
            item.codec.validate()?;
        }
        if self.operation == Operation::Unsubscribe && self.parent_request.as_deref().is_none_or(str::is_empty) {
            return Err(DeviceGatewayError::invalid("Unsubscribe requires parentRequestName"));
        }
        Ok(())
    }
    pub fn deadline(&self) -> Instant {
        Instant::now() + self.timeout
    }
}
#[derive(Clone, Debug, Default)]
pub struct RequestInput {
    pub values: BTreeMap<String, FieldValue>,
    /// A message sent as it is by a write without items (an MQTT publication on the request topic).
    pub payload: Option<Vec<u8>>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ItemStatus {
    Completed,
    Unchanged,
    Failed,
    Uncertain,
}
#[derive(Clone, Debug)]
pub struct ItemResult {
    pub parameter_id: String,
    pub value: Option<FieldValue>,
    pub status: ItemStatus,
    pub error: Option<String>,
    pub quality: Option<String>,
    pub source_time: Option<SystemTime>,
}
impl ItemResult {
    pub fn value(id: &str, value: FieldValue) -> Self {
        Self {
            parameter_id: id.into(),
            value: Some(value),
            status: ItemStatus::Completed,
            error: None,
            quality: None,
            source_time: None,
        }
    }
    pub fn error(id: &str, error: DeviceGatewayError) -> Self {
        Self {
            parameter_id: id.into(),
            value: None,
            status: if error.may_have_applied {
                ItemStatus::Uncertain
            } else {
                ItemStatus::Failed
            },
            error: Some(error.message),
            quality: None,
            source_time: None,
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct RequestOutput {
    pub items: Vec<ItemResult>,
    pub subscription_id: Option<String>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionOutcome {
    Completed,
    Partial,
    Active,
    Failed,
    Uncertain,
    Unchanged,
}
impl ExecutionOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Partial => "partial",
            Self::Active => "active",
            Self::Failed => "failed",
            Self::Uncertain => "uncertain",
            Self::Unchanged => "unchanged",
        }
    }
}
impl RequestOutput {
    pub fn outcome(&self) -> ExecutionOutcome {
        if self.subscription_id.is_some() {
            return ExecutionOutcome::Active;
        }
        if self.items.iter().any(|i| i.status == ItemStatus::Uncertain) {
            return ExecutionOutcome::Uncertain;
        }
        let failed = self.items.iter().filter(|i| i.status == ItemStatus::Failed).count();
        if failed > 0 {
            return if failed == self.items.len() {
                ExecutionOutcome::Failed
            } else {
                ExecutionOutcome::Partial
            };
        }
        if !self.items.is_empty() && self.items.iter().all(|i| i.status == ItemStatus::Unchanged) {
            ExecutionOutcome::Unchanged
        } else {
            ExecutionOutcome::Completed
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct Capabilities {
    pub read: bool,
    pub write: bool,
    pub subscribe: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    Invalid,
    Unsupported,
    Transport,
    Timeout,
    Busy,
    Persistence,
    Closed,
}
#[derive(Clone, Debug)]
pub struct DeviceGatewayError {
    pub kind: ErrorKind,
    pub message: String,
    pub may_have_applied: bool,
}
impl DeviceGatewayError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            may_have_applied: false,
        }
    }
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Invalid, message)
    }
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unsupported, message)
    }
    pub fn transport(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Transport, message)
    }
    pub fn timeout(writing: bool) -> Self {
        Self {
            kind: ErrorKind::Timeout,
            message: "Field operation exceeded its deadline".into(),
            may_have_applied: writing,
        }
    }
    pub fn uncertain(mut self) -> Self {
        self.may_have_applied = true;
        self
    }
}
impl fmt::Display for DeviceGatewayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl std::error::Error for DeviceGatewayError {}
#[derive(Clone, Debug)]
pub enum LogValueKind {
    Numeric,
    Enumeration,
    Message,
}
#[derive(Clone, Debug)]
pub struct Sample {
    pub device_id: String,
    pub log_value_kind: Option<LogValueKind>,
    pub parameter_id: Option<String>,
    pub value: FieldValue,
    pub source_time: Option<SystemTime>,
    pub received_time: SystemTime,
    pub quality: Option<String>,
}
#[derive(Clone, Debug)]
pub struct SampleBatch {
    pub request_name: String,
    /// Acquisition/decode failures do not discard other samples in the batch.
    pub errors: Vec<String>,
    pub logging: bool,
    pub request: Option<std::sync::Arc<PreparedDeviceRequest>>,
    pub samples: Vec<Sample>,
}
