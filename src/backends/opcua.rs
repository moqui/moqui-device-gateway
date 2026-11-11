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

//! OPC UA with async-opcua: reads and writes on a session shared per connection, and subscriptions of monitored items
//! on a session of their own, so that ending one cannot affect other requests. A lost subscription session ends the
//! subscription; moqui-device's start#Subscriptions starts it again. No write is sent twice.

use crate::{
    device_facade::{DeviceFacade, SampleSink},
    model::*,
};
use async_trait::async_trait;
use opcua::{
    client::{ClientBuilder, DataChangeCallback, IdentityToken, MonitoredItem, Session},
    types::*,
};
use std::{
    collections::HashMap,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub struct OpcUaDeviceClient {
    sessions: Mutex<HashMap<String, Arc<SessionGuard>>>,
}

struct SessionGuard {
    session: Arc<Session>,
    task: tokio::task::JoinHandle<StatusCode>,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub fn node_id(text: &str) -> Result<NodeId, DeviceGatewayError> {
    let text = text.trim();
    let text = text
        .strip_prefix("RAW(")
        .and_then(|t| t.strip_suffix(')'))
        .unwrap_or(text);
    NodeId::from_str(text)
        .map_err(|_| DeviceGatewayError::invalid("Invalid OPC UA NodeId; use an explicit namespace and identifier"))
}

pub fn from_variant(v: &Variant) -> Result<FieldValue, DeviceGatewayError> {
    Ok(match v {
        Variant::Empty => FieldValue::Null,
        Variant::Boolean(v) => FieldValue::Bool(*v),
        Variant::SByte(v) => FieldValue::Signed(*v as i64),
        Variant::Int16(v) => FieldValue::Signed(*v as i64),
        Variant::Int32(v) => FieldValue::Signed(*v as i64),
        Variant::Int64(v) => FieldValue::Signed(*v),
        Variant::Byte(v) => FieldValue::Unsigned(*v as u64),
        Variant::UInt16(v) => FieldValue::Unsigned(*v as u64),
        Variant::UInt32(v) => FieldValue::Unsigned(*v as u64),
        Variant::UInt64(v) => FieldValue::Unsigned(*v),
        Variant::Float(v) => FieldValue::Float(*v as f64),
        Variant::Double(v) => FieldValue::Float(*v),
        Variant::String(v) => FieldValue::Text(v.to_string()),
        Variant::ByteString(v) => FieldValue::Bytes(v.value.clone().unwrap_or_default()),
        _ => {
            return Err(DeviceGatewayError::unsupported(
                "Unsupported OPC UA variant type; no string coercion is applied",
            ));
        }
    })
}

pub fn to_variant(v: &FieldValue, ty: PhysicalType) -> Result<Variant, DeviceGatewayError> {
    let e = || DeviceGatewayError::invalid("Value does not match OPC UA physical type");
    let v = if matches!(ty, PhysicalType::Bool | PhysicalType::Text | PhysicalType::Bytes) {
        v.clone()
    } else {
        crate::codec::ValueCodec::default().encode(v, ParameterKind::Numeric, ty)?
    };
    Ok(match (ty, v) {
        (PhysicalType::Bool, FieldValue::Bool(v)) => Variant::Boolean(v),
        (PhysicalType::U8, FieldValue::Unsigned(v)) => Variant::Byte(v as u8),
        (PhysicalType::U16, FieldValue::Unsigned(v)) => Variant::UInt16(v as u16),
        (PhysicalType::U32, FieldValue::Unsigned(v)) => Variant::UInt32(v as u32),
        (PhysicalType::U64, FieldValue::Unsigned(v)) => Variant::UInt64(v),
        (PhysicalType::I8, FieldValue::Signed(v)) => Variant::SByte(v as i8),
        (PhysicalType::I16, FieldValue::Signed(v)) => Variant::Int16(v as i16),
        (PhysicalType::I32, FieldValue::Signed(v)) => Variant::Int32(v as i32),
        (PhysicalType::I64, FieldValue::Signed(v)) => Variant::Int64(v),
        (PhysicalType::F32, FieldValue::Float(v)) => Variant::Float(v as f32),
        (PhysicalType::F64, FieldValue::Float(v)) => Variant::Double(v),
        (PhysicalType::Text, FieldValue::Text(v)) => Variant::from(v.as_str()),
        (PhysicalType::Bytes, FieldValue::Bytes(v)) => Variant::ByteString(ByteString::from(v)),
        _ => return Err(e()),
    })
}

fn result(id: &str, v: DataValue) -> ItemResult {
    let status = v.status.unwrap_or(StatusCode::Good);
    let mut result = if !status.is_good() {
        ItemResult::error(id, DeviceGatewayError::transport(format!("OPC UA quality: {status}")))
    } else {
        match v.value.as_ref().map(from_variant).transpose() {
            Ok(Some(v)) => ItemResult::value(id, v),
            Ok(None) => ItemResult::error(id, DeviceGatewayError::transport("OPC UA result has no value")),
            Err(e) => ItemResult::error(id, e),
        }
    };
    result.quality = Some(status.to_string());
    result.source_time = v.source_timestamp.map(|t| SystemTime::from(t.as_chrono()));
    result
}

async fn connect(r: &PreparedDeviceRequest, deadline: Instant) -> Result<SessionGuard, DeviceGatewayError> {
    let c = &r.connection;
    let option = |k: &str| c.options.get(k).map(String::as_str);
    for key in c.options.keys() {
        if !matches!(
            key.as_str(),
            "securityPolicy" | "securityMode" | "pkiDir" | "certificateFile" | "privateKeyFile" | "applicationUri"
        ) {
            return Err(DeviceGatewayError::unsupported(format!("Unknown OPC UA option: {key}")));
        }
    }
    let url = url::Url::parse(&c.endpoint).map_err(|_| DeviceGatewayError::invalid("Invalid OPC UA endpoint"))?;
    if url.scheme() != "opc.tcp" || !url.username().is_empty() || url.password().is_some() || url.query().is_some() {
        return Err(DeviceGatewayError::invalid(
            "OPC UA endpoint must be opc.tcp:// without embedded credentials or options",
        ));
    }
    let policy = option("securityPolicy").unwrap_or("None");
    if !matches!(
        policy,
        "None" | "Basic256Sha256" | "Aes128_Sha256_RsaOaep" | "Aes256_Sha256_RsaPss"
    ) {
        return Err(DeviceGatewayError::unsupported(
            "Unsupported or deprecated OPC UA security policy",
        ));
    }
    let mode = match option("securityMode").unwrap_or("None") {
        "None" => MessageSecurityMode::None,
        "Sign" => MessageSecurityMode::Sign,
        "SignAndEncrypt" => MessageSecurityMode::SignAndEncrypt,
        _ => return Err(DeviceGatewayError::invalid("Invalid OPC UA security mode")),
    };
    if (policy == "None") != (mode == MessageSecurityMode::None) {
        return Err(DeviceGatewayError::invalid("OPC UA policy and security mode disagree"));
    }
    let mut builder = ClientBuilder::new()
        .application_name("moqui-device-gateway")
        .application_uri(option("applicationUri").unwrap_or("urn:moqui:device-gateway"))
        .pki_dir(option("pkiDir").unwrap_or("runtime/device-gateway/opcua-pki"))
        .trust_server_certs(false)
        .verify_server_certs(true)
        .create_sample_keypair(false)
        .request_timeout(r.timeout)
        .session_retry_limit(0);
    match (option("certificateFile"), option("privateKeyFile")) {
        (Some(c), Some(k)) => {
            builder = builder.certificate_path(c).private_key_path(k);
        }
        (None, None) if mode == MessageSecurityMode::None => {}
        _ => {
            return Err(DeviceGatewayError::invalid(
                "Secure OPC UA requires certificateFile and privateKeyFile",
            ));
        }
    }
    let (identity, token_type) = match (&c.credentials.username, &c.credentials.password) {
        (None, None) => (IdentityToken::Anonymous, UserTokenType::Anonymous),
        (Some(u), Some(p)) if mode == MessageSecurityMode::SignAndEncrypt => {
            (IdentityToken::new_user_name(u, p), UserTokenType::UserName)
        }
        _ => {
            return Err(DeviceGatewayError::invalid("Username/password require SignAndEncrypt"));
        }
    };
    let mut token = UserTokenPolicy::anonymous();
    token.token_type = token_type;
    let policy = format!("http://opcfoundation.org/UA/SecurityPolicy#{policy}");
    let endpoint: EndpointDescription = (c.endpoint.as_str(), policy.as_str(), mode, token).into();
    let mut client = builder
        .client()
        .map_err(|e| DeviceGatewayError::invalid(format!("Invalid OPC UA client configuration: {e:?}")))?;
    let (session, event_loop) =
        tokio::time::timeout_at(deadline.into(), client.connect_to_matching_endpoint(endpoint, identity))
            .await
            .map_err(|_| DeviceGatewayError::timeout(false))?
            .map_err(|e| DeviceGatewayError::transport(format!("OPC UA endpoint connection failed: {e}")))?;
    let guard = SessionGuard {
        session: session.clone(),
        task: event_loop.spawn(),
    };
    if !tokio::time::timeout_at(deadline.into(), session.wait_for_connection())
        .await
        .map_err(|_| DeviceGatewayError::timeout(false))?
    {
        return Err(DeviceGatewayError::transport("OPC UA session did not connect"));
    }
    Ok(guard)
}

impl OpcUaDeviceClient {
    async fn session(
        &self,
        r: &PreparedDeviceRequest,
        deadline: Instant,
    ) -> Result<Arc<SessionGuard>, DeviceGatewayError> {
        let mut sessions = tokio::time::timeout_at(deadline.into(), self.sessions.lock())
            .await
            .map_err(|_| DeviceGatewayError::timeout(false))?;
        let key = r.connection.session_key();
        if let Some(s) = sessions.get(&key).filter(|s| !s.task.is_finished()) {
            return Ok(s.clone());
        }
        let s = Arc::new(connect(r, deadline).await?);
        sessions.insert(key, s.clone());
        Ok(s)
    }
}

#[async_trait]
impl DeviceFacade for OpcUaDeviceClient {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            read: true,
            write: true,
            subscribe: true,
        }
    }

    async fn execute(
        &self,
        r: &PreparedDeviceRequest,
        input: &RequestInput,
        out: &mut RequestOutput,
        deadline: Instant,
    ) -> Result<(), DeviceGatewayError> {
        let nodes = r
            .items
            .iter()
            .map(|i| node_id(&i.address))
            .collect::<Result<Vec<_>, _>>()?;
        if nodes.is_empty() {
            return Err(DeviceGatewayError::invalid("OPC UA request has no items"));
        }
        let writing = r.operation == Operation::Write;
        let writes = if writing {
            r.items
                .iter()
                .zip(&nodes)
                .map(|(i, n)| {
                    Ok(WriteValue {
                        node_id: n.clone(),
                        attribute_id: AttributeId::Value as u32,
                        index_range: NumericRange::None,
                        value: DataValue::value_only(to_variant(
                            input
                                .values
                                .get(&i.parameter_id)
                                .ok_or_else(|| DeviceGatewayError::invalid("Missing OPC UA write value"))?,
                            i.physical_type,
                        )?),
                    })
                })
                .collect::<Result<Vec<_>, DeviceGatewayError>>()?
        } else {
            vec![]
        };
        let guard = self.session(r, deadline).await?;
        let s = &guard.session;
        if writing {
            let statuses = tokio::time::timeout_at(deadline.into(), s.write(&writes))
                .await
                .map_err(|_| DeviceGatewayError::timeout(true))?
                .map_err(|e| DeviceGatewayError::transport(format!("OPC UA write failed: {e}")).uncertain())?;
            if statuses.len() != r.items.len() {
                return Err(DeviceGatewayError::transport("OPC UA write result count differs").uncertain());
            }
            for (i, status) in r.items.iter().zip(statuses) {
                out.items.push(if status.is_good() {
                    ItemResult::value(&i.parameter_id, input.values[&i.parameter_id].clone())
                } else {
                    ItemResult::error(
                        &i.parameter_id,
                        DeviceGatewayError::transport(format!("OPC UA write rejected: {status}")),
                    )
                });
            }
        } else {
            let nodes = nodes.into_iter().map(ReadValueId::from).collect::<Vec<_>>();
            let values = tokio::time::timeout_at(deadline.into(), s.read(&nodes, TimestampsToReturn::Both, 0.0))
                .await
                .map_err(|_| DeviceGatewayError::timeout(false))?
                .map_err(|e| DeviceGatewayError::transport(format!("OPC UA read failed: {e}")))?;
            if values.len() != r.items.len() {
                return Err(DeviceGatewayError::transport("OPC UA read result count differs"));
            }
            for (i, v) in r.items.iter().zip(values) {
                out.items.push(result(&i.parameter_id, v));
            }
        }
        Ok(())
    }

    async fn subscribe(
        &self,
        r: Arc<PreparedDeviceRequest>,
        sink: Arc<dyn SampleSink>,
        cancel: CancellationToken,
        deadline: Instant,
    ) -> Result<tokio::task::JoinHandle<()>, DeviceGatewayError> {
        let nodes = r
            .items
            .iter()
            .map(|i| node_id(&i.address))
            .collect::<Result<Vec<_>, _>>()?;
        if nodes.is_empty() {
            return Err(DeviceGatewayError::invalid("OPC UA subscription has no items"));
        }
        if r.subscription_type == SubscriptionType::Event {
            return Err(DeviceGatewayError::unsupported(
                "OPC UA events are not implemented: use DrtStateChange, DrtSubscribe or DrtCyclic",
            ));
        }
        // A cyclic subscription samples each item at the polling interval; a change of state lets the server sample
        // at its fastest rate and report the changes.
        let sampling_interval = match r.subscription_type {
            SubscriptionType::Cyclic => r.polling_interval.as_secs_f64() * 1000.0,
            _ => 0.0,
        };
        // A subscription owns a dedicated session, so cancellation cannot affect other requests.
        let mut guard = connect(&r, deadline).await?;
        let req = r.clone();
        let ids = nodes.clone();
        let callback = DataChangeCallback::new(move |value: DataValue, item: &MonitoredItem| {
            let Some(index) = ids.iter().position(|id| id == &item.item_to_monitor().node_id) else {
                return;
            };
            let value = result(&req.items[index].parameter_id, value);
            if let Some(error) = value.error {
                // A bad quality goes to the store as an acquisition error, which logs and notifies it.
                tracing::debug!(request = %req.name, error = %error, "OPC UA sample without a good value");
                let _ = sink.emit(SampleBatch {
                    request_name: req.name.clone(),
                    errors: vec![format!("{}: {}", value.parameter_id, error)],
                    logging: req.logging,
                    request: Some(req.clone()),
                    samples: Vec::new(),
                });
                return;
            }
            let Some(value_data) = value.value else {
                return;
            };
            if let Err(error) = sink.emit(SampleBatch {
                errors: Vec::new(),
                request_name: req.name.clone(),
                logging: req.logging,
                request: Some(req.clone()),
                samples: vec![Sample {
                    device_id: req.device_id.clone(),
                    log_value_kind: None,
                    parameter_id: Some(value.parameter_id),
                    value: value_data,
                    source_time: value.source_time,
                    received_time: SystemTime::now(),
                    quality: value.quality,
                }],
            }) {
                tracing::error!(request = %req.name, error = %error, "OPC UA sample not queued");
            }
        });
        let session = &guard.session;
        let sub = tokio::time::timeout_at(
            deadline.into(),
            session.create_subscription(r.polling_interval, 10, 30, 0, 0, true, callback),
        )
        .await
        .map_err(|_| DeviceGatewayError::timeout(false))?
        .map_err(|e| DeviceGatewayError::transport(format!("Cannot create OPC UA subscription: {e}")))?;
        let items = nodes
            .into_iter()
            .map(|n| {
                MonitoredItemCreateRequest::new(
                    ReadValueId::from(n),
                    MonitoringMode::Reporting,
                    MonitoringParameters {
                        sampling_interval,
                        ..Default::default()
                    },
                )
            })
            .collect::<Vec<_>>();
        let results = tokio::time::timeout_at(
            deadline.into(),
            session.create_monitored_items(sub, TimestampsToReturn::Both, items),
        )
        .await
        .map_err(|_| DeviceGatewayError::timeout(false))?
        .map_err(|e| DeviceGatewayError::transport(format!("Cannot create OPC UA monitored items: {e}")))?;
        if results.len() != r.items.len() {
            return Err(DeviceGatewayError::transport("OPC UA monitored item count differs"));
        }
        if let Some((item, rejected)) = r
            .items
            .iter()
            .zip(&results)
            .find(|(_, i)| !i.result.status_code.is_good())
        {
            return Err(DeviceGatewayError::transport(format!(
                "OPC UA rejected the monitored item of {}: {}",
                item.parameter_id, rejected.result.status_code
            )));
        }
        Ok(tokio::spawn(async move {
            tokio::select! {
                _ = cancel.cancelled() => {}
                _ = &mut guard.task => {
                    tracing::error!(request = %r.name, "OPC UA subscription session stopped");
                }
            }
            let _ = tokio::time::timeout(Duration::from_secs(2), guard.session.delete_subscriptions(&[sub])).await;
            let _ = tokio::time::timeout(Duration::from_secs(2), guard.session.disconnect()).await;
        }))
    }

    async fn close(&self) {
        let sessions = std::mem::take(&mut *self.sessions.lock().await);
        for (_, s) in sessions {
            let _ = tokio::time::timeout(Duration::from_secs(2), s.session.disconnect()).await;
        }
    }
}
