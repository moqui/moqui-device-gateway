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

//! Version-specific rumqttc APIs behind one owned MQTT session. No Moqui runtime is required.
use crate::model::*;
use rumqttc::{v5, Outgoing, Transport};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

pub(super) enum Session {
    V3(rumqttc::AsyncClient, Box<rumqttc::EventLoop>),
    V5(v5::AsyncClient, Box<v5::EventLoop>),
}

pub(super) enum Event {
    Connected,
    /// A message, with what acknowledges it when it was sent at QoS 1 or 2.
    Message(String, Vec<u8>, Option<Ack>),
    Subscribed(bool),
    Unsubscribed(bool),
    Published(bool),
    Other,
}

/// An incoming message to acknowledge once the host is done with it.
pub(super) enum Ack {
    V3(Box<rumqttc::Publish>),
    V5(Box<v5::mqttbytes::v5::Publish>),
}

/// Acknowledges incoming messages beside the task that polls the session.
pub(super) enum Acker {
    V3(rumqttc::AsyncClient),
    V5(v5::AsyncClient),
}

impl Acker {
    pub async fn ack(&self, ack: Ack) -> Result<(), DeviceGatewayError> {
        let failed = || DeviceGatewayError::transport("Cannot queue MQTT acknowledgement");
        match (self, ack) {
            (Self::V3(c), Ack::V3(p)) => c.ack(&p).await.map_err(|_| failed()),
            (Self::V5(c), Ack::V5(p)) => c.ack(&p).await.map_err(|_| failed()),
            _ => Err(DeviceGatewayError::invalid(
                "MQTT acknowledgement of another protocol version",
            )),
        }
    }
}

pub(super) struct Settings {
    pub qos: u8,
    /// A subscription in a persistent session at QoS 1 or 2: a message is acknowledged only once stored, so the broker
    /// keeps what the gateway has not stored, across a network outage, a database outage or a gateway restart.
    pub durable: bool,
    /// The unacknowledged messages a durable subscription holds, which MQTT 5 asks the broker to respect.
    pub receive_maximum: u16,
    pub retain: bool,
    pub reconnect: bool,
    pub reconnect_delay: Duration,
    pub version: u8,
    clean: bool,
    expiry: u32,
    keep: u64,
}

fn boolean(r: &PreparedDeviceRequest, name: &str, default: bool) -> Result<bool, DeviceGatewayError> {
    match r.connection.options.get(name).map(String::as_str) {
        None => Ok(default),
        Some("true" | "Y") => Ok(true),
        Some("false" | "N") => Ok(false),
        _ => Err(DeviceGatewayError::invalid(format!(
            "Invalid MQTT boolean option: {name}"
        ))),
    }
}

impl Settings {
    pub fn parse(r: &PreparedDeviceRequest) -> Result<Self, DeviceGatewayError> {
        for key in r.connection.options.keys() {
            if !matches!(
                key.as_str(),
                "client-id"
                    | "client-id-prefix"
                    | "keep-alive-seconds"
                    | "retain"
                    | "qos"
                    | "protocol-version"
                    | "clean-start"
                    | "session-expiry-seconds"
                    | "automatic-reconnect"
                    | "max-reconnect-delay-ms"
                    | "receive-maximum"
                    | "ca-file"
                    | "client-cert-file"
                    | "client-key-file"
            ) {
                return Err(DeviceGatewayError::unsupported(format!("Unknown MQTT option: {key}")));
            }
        }
        let number = |name: &str, default: u64| -> Result<u64, DeviceGatewayError> {
            r.connection
                .options
                .get(name)
                .map(|s| s.parse())
                .transpose()
                .map_err(|_| DeviceGatewayError::invalid(format!("Invalid MQTT option: {name}")))
                .map(|v| v.unwrap_or(default))
        };
        let version = number("protocol-version", 5)?;
        let qos = number("qos", 1)?;
        let keep = number("keep-alive-seconds", 30)?;
        let delay = number("max-reconnect-delay-ms", 5000)?;
        let expiry = number("session-expiry-seconds", 86400)?;
        let receive_maximum = number("receive-maximum", 256)?;
        if !matches!(version, 3 | 5)
            || qos > 2
            || !(1..=u16::MAX as u64).contains(&receive_maximum)
            || !(5..=3600).contains(&keep)
            || !(100..=60000).contains(&delay)
            || expiry > u32::MAX as u64
        {
            return Err(DeviceGatewayError::invalid(
                "Invalid MQTT version, QoS, keepalive or session limits",
            ));
        }
        let clean = boolean(r, "clean-start", r.operation != Operation::Subscribe)?;
        Ok(Self {
            version: version as u8,
            qos: qos as u8,
            durable: r.operation == Operation::Subscribe && !clean && qos > 0,
            receive_maximum: receive_maximum as u16,
            keep,
            expiry: expiry as u32,
            retain: boolean(r, "retain", false)?,
            clean,
            reconnect: boolean(r, "automatic-reconnect", true)?,
            reconnect_delay: Duration::from_millis(delay),
        })
    }
}

pub(super) fn client_id(r: &PreparedDeviceRequest) -> String {
    if let Some(id) = r.connection.options.get("client-id") {
        return id.clone();
    }
    let prefix = r
        .connection
        .options
        .get("client-id-prefix")
        .map(String::as_str)
        .unwrap_or("moqui-gw");
    // Encode identities instead of replacing punctuation: distinct request names must not collide.
    let encode = |s: &str| s.bytes().map(|b| format!("{b:02x}")).collect::<String>();
    let stable = format!("{prefix}-{}-{}", encode(&r.device_id), encode(&r.name));
    if r.operation == Operation::Subscribe {
        stable
    } else {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        format!(
            "{stable}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )
    }
}

impl Session {
    pub fn new(r: &PreparedDeviceRequest, config: &Settings) -> Result<Self, DeviceGatewayError> {
        let url = url::Url::parse(&r.connection.endpoint)
            .map_err(|_| DeviceGatewayError::invalid("MQTT endpoint must be mqtt:// or mqtts://"))?;
        if !matches!(url.scheme(), "mqtt" | "mqtts")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || !matches!(url.path(), "" | "/")
        {
            return Err(DeviceGatewayError::invalid(
                "Configure MQTT credentials, topics and options separately",
            ));
        }
        let host = url
            .host_str()
            .ok_or_else(|| DeviceGatewayError::invalid("Missing MQTT host"))?;
        let port = url.port().unwrap_or(if url.scheme() == "mqtts" { 8883 } else { 1883 });
        let id = client_id(r);
        if id.is_empty() || id.len() > u16::MAX as usize {
            return Err(DeviceGatewayError::invalid(
                "MQTT client ID must be nonempty and fit a UTF-8 string",
            ));
        }
        let option = |name: &str| r.connection.options.get(name);
        let transport = if url.scheme() == "mqtts" {
            if let Some(ca) = option("ca-file") {
                let read = |path: &str| {
                    std::fs::read(path)
                        .map_err(|_| DeviceGatewayError::invalid("Cannot read configured MQTT TLS credential file"))
                };
                let client = match (option("client-cert-file"), option("client-key-file")) {
                    (Some(cert), Some(key)) => Some((read(cert)?, read(key)?)),
                    (None, None) => None,
                    _ => {
                        return Err(DeviceGatewayError::invalid(
                            "MQTT client certificate and key must be paired",
                        ))
                    }
                };
                Transport::tls(read(ca)?, client, None)
            } else {
                if option("client-cert-file").is_some() || option("client-key-file").is_some() {
                    return Err(DeviceGatewayError::invalid("MQTT mutual TLS requires a CA file"));
                }
                Transport::tls_with_default_config()
            }
        } else {
            if option("ca-file").is_some()
                || option("client-cert-file").is_some()
                || option("client-key-file").is_some()
            {
                return Err(DeviceGatewayError::invalid("TLS credentials require mqtts://"));
            }
            Transport::Tcp
        };
        if config.version == 3 {
            let mut opts = rumqttc::MqttOptions::new(id, host, port);
            opts.set_transport(transport)
                .set_keep_alive(Duration::from_secs(config.keep))
                .set_clean_session(config.clean)
                .set_manual_acks(config.durable);
            if let Some(user) = &r.connection.credentials.username {
                opts.set_credentials(user, r.connection.credentials.password.as_deref().unwrap_or(""));
            }
            let (client, events) = rumqttc::AsyncClient::new(opts, 32);
            Ok(Self::V3(client, Box::new(events)))
        } else {
            let mut opts = v5::MqttOptions::new(id, host, port);
            opts.set_transport(transport)
                .set_keep_alive(Duration::from_secs(config.keep))
                .set_clean_start(config.clean);
            if !config.clean {
                opts.set_session_expiry_interval(Some(config.expiry));
            }
            if config.durable {
                opts.set_manual_acks(true)
                    .set_receive_maximum(Some(config.receive_maximum));
            }
            if let Some(user) = &r.connection.credentials.username {
                opts.set_credentials(user, r.connection.credentials.password.as_deref().unwrap_or(""));
            }
            let (client, events) = v5::AsyncClient::new(opts, 32);
            Ok(Self::V5(client, Box::new(events)))
        }
    }

    pub async fn publish(&mut self, topic: &str, bytes: Vec<u8>, config: &Settings) -> Result<(), DeviceGatewayError> {
        let failed = |_| DeviceGatewayError::transport("Cannot queue MQTT publication");
        match self {
            Self::V3(c, _) => c
                .publish(
                    topic,
                    match config.qos {
                        0 => rumqttc::QoS::AtMostOnce,
                        1 => rumqttc::QoS::AtLeastOnce,
                        _ => rumqttc::QoS::ExactlyOnce,
                    },
                    config.retain,
                    bytes,
                )
                .await
                .map_err(failed),
            Self::V5(c, _) => c
                .publish(
                    topic,
                    match config.qos {
                        0 => v5::mqttbytes::QoS::AtMostOnce,
                        1 => v5::mqttbytes::QoS::AtLeastOnce,
                        _ => v5::mqttbytes::QoS::ExactlyOnce,
                    },
                    config.retain,
                    bytes,
                )
                .await
                .map_err(|_| DeviceGatewayError::transport("Cannot queue MQTT publication")),
        }
    }

    pub async fn subscribe(&mut self, topic: &str, qos: u8) -> Result<(), DeviceGatewayError> {
        match self {
            Self::V3(c, _) => c
                .subscribe(
                    topic,
                    match qos {
                        0 => rumqttc::QoS::AtMostOnce,
                        1 => rumqttc::QoS::AtLeastOnce,
                        _ => rumqttc::QoS::ExactlyOnce,
                    },
                )
                .await
                .map_err(|_| DeviceGatewayError::transport("Cannot queue MQTT subscription")),
            Self::V5(c, _) => c
                .subscribe(
                    topic,
                    match qos {
                        0 => v5::mqttbytes::QoS::AtMostOnce,
                        1 => v5::mqttbytes::QoS::AtLeastOnce,
                        _ => v5::mqttbytes::QoS::ExactlyOnce,
                    },
                )
                .await
                .map_err(|_| DeviceGatewayError::transport("Cannot queue MQTT subscription")),
        }
    }

    pub async fn unsubscribe(&mut self, topic: &str) -> Result<(), DeviceGatewayError> {
        match self {
            Self::V3(c, _) => c
                .unsubscribe(topic)
                .await
                .map_err(|_| DeviceGatewayError::transport("Cannot queue MQTT unsubscribe")),
            Self::V5(c, _) => c
                .unsubscribe(topic)
                .await
                .map_err(|_| DeviceGatewayError::transport("Cannot queue MQTT unsubscribe")),
        }
    }

    pub async fn disconnect(&mut self) -> Result<(), DeviceGatewayError> {
        let failed = |_| DeviceGatewayError::transport("Cannot queue MQTT disconnect");
        match self {
            Self::V3(c, _) => c.disconnect().await.map_err(failed),
            Self::V5(c, _) => c
                .disconnect()
                .await
                .map_err(|_| DeviceGatewayError::transport("Cannot queue MQTT disconnect")),
        }
    }

    pub fn acker(&self) -> Acker {
        match self {
            Self::V3(c, _) => Acker::V3(c.clone()),
            Self::V5(c, _) => Acker::V5(c.clone()),
        }
    }

    pub async fn poll(&mut self) -> Result<Event, DeviceGatewayError> {
        use v5::mqttbytes::v5::{Packet, PubAckReason, PubCompReason, PubRecReason, SubscribeReasonCode};
        match self {
            Self::V3(_, events) => match events.poll().await {
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => Ok(Event::Connected),
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(p))) => {
                    let (topic, bytes) = (p.topic.clone(), p.payload.to_vec());
                    let ack = (p.qos != rumqttc::QoS::AtMostOnce).then(|| Ack::V3(Box::new(p)));
                    Ok(Event::Message(topic, bytes, ack))
                }
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::SubAck(p))) => Ok(Event::Subscribed(
                    p.return_codes.len() == 1 && matches!(p.return_codes[0], rumqttc::SubscribeReasonCode::Success(_)),
                )),
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::UnsubAck(_))) => Ok(Event::Unsubscribed(true)),
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_) | rumqttc::Packet::PubComp(_)))
                | Ok(rumqttc::Event::Outgoing(Outgoing::Publish(0))) => Ok(Event::Published(true)),
                Ok(_) => Ok(Event::Other),
                Err(error) => {
                    tracing::debug!(error=%error, "MQTT connection failed");
                    Err(DeviceGatewayError::transport(format!(
                        "MQTT connection failed: {error}"
                    )))
                }
            },
            Self::V5(_, events) => match events.poll().await {
                Ok(v5::Event::Incoming(Packet::ConnAck(_))) => Ok(Event::Connected),
                Ok(v5::Event::Incoming(Packet::Publish(p))) => {
                    let (topic, bytes) = (String::from_utf8_lossy(&p.topic).into_owned(), p.payload.to_vec());
                    let ack = (p.qos != v5::mqttbytes::QoS::AtMostOnce).then(|| Ack::V5(Box::new(p)));
                    Ok(Event::Message(topic, bytes, ack))
                }
                Ok(v5::Event::Incoming(Packet::SubAck(p))) => Ok(Event::Subscribed(
                    p.return_codes.len() == 1 && matches!(p.return_codes[0], SubscribeReasonCode::Success(_)),
                )),
                Ok(v5::Event::Incoming(Packet::UnsubAck(p))) => Ok(Event::Unsubscribed(
                    p.reasons.len() == 1
                        && p.reasons.iter().all(|reason| {
                            matches!(
                                reason,
                                v5::mqttbytes::v5::UnsubAckReason::Success
                                    | v5::mqttbytes::v5::UnsubAckReason::NoSubscriptionExisted
                            )
                        }),
                )),
                Ok(v5::Event::Incoming(Packet::PubAck(p))) => Ok(Event::Published(matches!(
                    p.reason,
                    PubAckReason::Success | PubAckReason::NoMatchingSubscribers
                ))),
                Ok(v5::Event::Incoming(Packet::PubComp(p))) => Ok(Event::Published(p.reason == PubCompReason::Success)),
                Ok(v5::Event::Incoming(Packet::PubRec(p)))
                    if !matches!(p.reason, PubRecReason::Success | PubRecReason::NoMatchingSubscribers) =>
                {
                    Ok(Event::Published(false))
                }
                Ok(v5::Event::Outgoing(Outgoing::Publish(0))) => Ok(Event::Published(true)),
                Ok(_) => Ok(Event::Other),
                Err(error) => {
                    tracing::debug!(error=%error, "MQTT connection failed");
                    Err(DeviceGatewayError::transport(format!(
                        "MQTT connection failed: {error}"
                    )))
                }
            },
        }
    }
}
