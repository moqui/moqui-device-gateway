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

//! MQTT 3.1.1 and 5 with rumqttc: publications, and subscriptions that survive a broker or network outage. A
//! durable subscription acknowledges a message only once the host has stored it. A topic is not read: MQTT is
//! subscribed.
use super::mqtt_session::{Ack, Acker, Event, Session, Settings};
use crate::{
    device_facade::{DeviceFacade, Receipt, SampleSink},
    model::*,
    payload,
};
use async_trait::async_trait;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Instant, SystemTime},
};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub struct MqttDeviceClient {
    /// Set when the host shuts down: subscriptions then end without unsubscribing, so each persistent session keeps
    /// collecting in the broker until the gateway is back.
    shutdown: Arc<AtomicBool>,
}

fn topic(r: &PreparedDeviceRequest, item: Option<&Item>) -> Result<String, DeviceGatewayError> {
    item.map(|i| i.address.as_str())
        .filter(|s| !s.is_empty())
        .or(r.query.as_deref())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .ok_or_else(|| DeviceGatewayError::invalid("MQTT topic is required"))
}

async fn poll(session: &mut Session, deadline: Instant, writing: bool) -> Result<Event, DeviceGatewayError> {
    tokio::time::timeout_at(deadline.into(), session.poll())
        .await
        .map_err(|_| DeviceGatewayError::timeout(writing))?
        .map_err(|e| if writing { e.uncertain() } else { e })
}

async fn subscribe_topics(
    session: &mut Session,
    topics: &[String],
    qos: u8,
    deadline: Instant,
) -> Result<Vec<(String, Vec<u8>, Option<Ack>)>, DeviceGatewayError> {
    let mut pending = Vec::new();
    for topic in topics {
        session.subscribe(topic, qos).await?;
        loop {
            match poll(session, deadline, false).await? {
                Event::Subscribed(true) => break,
                Event::Subscribed(false) => {
                    return Err(DeviceGatewayError::transport("MQTT broker rejected subscription"))
                }
                Event::Message(topic, bytes, ack) => pending.push((topic, bytes, ack)),
                _ => {}
            }
        }
    }
    Ok(pending)
}

fn topics(r: &PreparedDeviceRequest) -> Result<Vec<String>, DeviceGatewayError> {
    let mut topics = Vec::new();
    if r.plc_log {
        topics.push(topic(r, None)?);
    } else {
        for item in &r.items {
            let topic = topic(r, Some(item))?;
            if !topics.contains(&topic) {
                topics.push(topic);
            }
        }
    }
    if topics.is_empty() {
        return Err(DeviceGatewayError::invalid("MQTT request has no topics"));
    }
    Ok(topics)
}

pub(crate) fn matches_topic(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') && !filter.starts_with('$') {
        return false;
    }
    let mut f = filter.split('/');
    let mut t = topic.split('/');
    loop {
        match (f.next(), t.next()) {
            (Some("#"), _) => return f.next().is_none(),
            (Some("+"), Some(_)) => {}
            (Some(a), Some(b)) if a == b => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

/// The values of the items whose topic filter matches the topic of a message.
fn values(r: &PreparedDeviceRequest, topic_name: &str, bytes: &[u8]) -> Result<Vec<ItemResult>, DeviceGatewayError> {
    let selected = r
        .items
        .iter()
        .filter(|i| topic(r, Some(i)).is_ok_and(|t| matches_topic(&t, topic_name)))
        .collect::<Vec<_>>();
    payload::read_items(&selected, bytes)
}

fn emit(r: &PreparedDeviceRequest, topic: &str, bytes: &[u8], sink: &dyn SampleSink) -> Result<(), DeviceGatewayError> {
    sink.emit(batch_of(r, topic, bytes))
}

/// The batch of one message. A message that does not decode is a batch of its error: stored as a failure, it is
/// acknowledged and does not block the session. The facade attaches the request to the batch.
fn batch_of(r: &PreparedDeviceRequest, topic: &str, bytes: &[u8]) -> SampleBatch {
    if r.plc_log {
        return payload::plc_log_batch(r, bytes).unwrap_or_else(|error| SampleBatch {
            request_name: r.name.clone(),
            errors: vec![error.to_string()],
            logging: true,
            request: None,
            samples: Vec::new(),
        });
    }
    let mut samples = Vec::new();
    let mut errors = Vec::new();
    let items = match values(r, topic, bytes) {
        Ok(items) => items,
        Err(error) => {
            errors.push(error.to_string());
            Vec::new()
        }
    };
    for item in items {
        if let Some(error) = item.error {
            errors.push(format!("{}: {}", item.parameter_id, error));
            continue;
        }
        if let Some(value) = item.value {
            samples.push(Sample {
                device_id: r.device_id.clone(),
                log_value_kind: None,
                parameter_id: Some(item.parameter_id),
                value,
                source_time: item.source_time,
                received_time: SystemTime::now(),
                quality: item.quality,
            });
        }
    }
    SampleBatch {
        errors,
        request_name: r.name.clone(),
        logging: r.logging,
        request: None,
        samples,
    }
}

/// What a message of a durable subscription waits for before the broker may forget it: its delivery to the host, then
/// the host's receipt. Its permit bounds the messages held between the broker and the database.
type Held = (String, Vec<u8>, Option<Ack>, OwnedSemaphorePermit);

/// The delivery side of a durable subscription. One task delivers the messages in the order received, waiting for
/// room in the host's queue; another acknowledges them in that order, each once the host is done with it. The polling
/// task meanwhile keeps the session alive. A message the host gave up on (a shutdown) stays unacknowledged, and the
/// broker delivers it again on the next connection of the session.
struct Durable {
    deliveries: mpsc::UnboundedSender<Held>,
    permits: Arc<Semaphore>,
    permit: Option<OwnedSemaphorePermit>,
    acknowledged: tokio::task::JoinHandle<()>,
}

impl Durable {
    fn start(r: Arc<PreparedDeviceRequest>, sink: Arc<dyn SampleSink>, acker: Acker, limit: usize) -> Self {
        let (deliveries, mut received) = mpsc::unbounded_channel::<Held>();
        let (delivered, mut acknowledging) =
            mpsc::unbounded_channel::<(Option<Ack>, Option<Receipt>, OwnedSemaphorePermit)>();
        let name = r.name.clone();
        tokio::spawn(async move {
            while let Some((topic, bytes, ack, permit)) = received.recv().await {
                let receipt = match sink.deliver(batch_of(&r, &topic, &bytes)).await {
                    Ok(receipt) => Some(receipt),
                    Err(error) => {
                        tracing::warn!(request=%r.name,error=%error,"MQTT message not delivered; the broker keeps it");
                        None
                    }
                };
                if delivered.send((ack, receipt, permit)).is_err() {
                    break;
                }
            }
        });
        let acknowledged = tokio::spawn(async move {
            while let Some((ack, receipt, permit)) = acknowledging.recv().await {
                let finished = match receipt {
                    Some(receipt) => receipt.finished().await,
                    None => false,
                };
                // The polling task sends the acknowledgement: release the permit first, so it can poll.
                drop(permit);
                if let (true, Some(ack)) = (finished, ack) {
                    if let Err(error) = acker.ack(ack).await {
                        tracing::warn!(request=%name,error=%error,"MQTT acknowledgement not sent; the broker delivers the message again");
                    }
                }
            }
        });
        Self {
            deliveries,
            permits: Arc::new(Semaphore::new(limit)),
            permit: None,
            acknowledged,
        }
    }

    /// Deliver nothing more, and end once every message delivered so far is acknowledged or given up.
    fn finish(self) -> tokio::task::JoinHandle<()> {
        self.acknowledged
    }

    /// Wait for room for one more message; false when cancelled.
    async fn reserve(&mut self, cancel: &CancellationToken) -> bool {
        if self.permit.is_none() {
            tokio::select! { biased;
                _ = cancel.cancelled() => return false,
                permit = self.permits.clone().acquire_owned() => self.permit = permit.ok(),
            }
        }
        self.permit.is_some()
    }

    async fn hold(&mut self, cancel: &CancellationToken, topic: String, bytes: Vec<u8>, ack: Option<Ack>) -> bool {
        if !self.reserve(cancel).await {
            return false;
        }
        let Some(permit) = self.permit.take() else {
            return false;
        };
        self.deliveries.send((topic, bytes, ack, permit)).is_ok()
    }
}

/// A received message: held until stored on a durable subscription, otherwise emitted (acknowledged on receipt).
async fn forward(
    durable: &mut Option<Durable>,
    cancel: &CancellationToken,
    r: &PreparedDeviceRequest,
    sink: &dyn SampleSink,
    (topic, bytes, ack): (String, Vec<u8>, Option<Ack>),
    recovered: bool,
) -> bool {
    match durable {
        Some(durable) => durable.hold(cancel, topic, bytes, ack).await,
        None => {
            if let Err(error) = emit(r, &topic, &bytes, sink) {
                if recovered {
                    tracing::error!(request=%r.name,error=%error,"MQTT recovered sample rejected");
                } else {
                    tracing::error!(request=%r.name,error=%error,"MQTT sample rejected");
                }
            }
            true
        }
    }
}

async fn publish(
    session: &mut Session,
    topic: &str,
    bytes: Vec<u8>,
    config: &Settings,
    deadline: Instant,
) -> Result<(), DeviceGatewayError> {
    session.publish(topic, bytes, config).await?;
    loop {
        match poll(session, deadline, true).await? {
            Event::Published(true) => return Ok(()),
            Event::Published(false) => return Err(DeviceGatewayError::transport("MQTT broker rejected publication")),
            _ => {}
        }
    }
}

fn publish_topic(topic: &str) -> Result<(), DeviceGatewayError> {
    if topic.is_empty() || topic.contains(['#', '+', '\0']) || topic.len() > u16::MAX as usize {
        return Err(DeviceGatewayError::invalid("Invalid MQTT publication topic"));
    }
    Ok(())
}

#[async_trait]
impl DeviceFacade for MqttDeviceClient {
    fn shutting_down(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    fn capabilities(&self) -> Capabilities {
        // A topic has no value to read: a DrtRead, and so polling, is an error; subscribe to the topic instead.
        Capabilities {
            read: false,
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
        if r.operation != Operation::Write {
            return Err(DeviceGatewayError::unsupported(
                "MQTT has no read: subscribe to the topic (DrtSubscribe or DrtEvent)",
            ));
        }
        let config = Settings::parse(r)?;
        if let Some(bytes) = &input.payload {
            let topic = topic(r, None)?;
            publish_topic(&topic)?;
            let mut session = Session::new(r, &config)?;
            return publish(&mut session, &topic, bytes.clone(), &config, deadline).await;
        }
        // Validate every command before sending the first one. Never retry a finite write.
        let plans = r
            .items
            .iter()
            .map(|item| {
                let topic = topic(r, Some(item))?;
                publish_topic(&topic)?;
                Ok((item, topic, payload::write_item_payload(item, input)?))
            })
            .collect::<Result<Vec<_>, DeviceGatewayError>>()?;
        if plans.is_empty() {
            return Err(DeviceGatewayError::invalid("MQTT write has no items"));
        }
        let mut session = Session::new(r, &config)?;
        for (item, topic, bytes) in plans {
            match publish(&mut session, &topic, bytes, &config, deadline).await {
                Ok(()) => out.items.push(ItemResult::value(
                    &item.parameter_id,
                    input.values[&item.parameter_id].clone(),
                )),
                Err(error) => {
                    out.items.push(ItemResult::error(&item.parameter_id, error.clone()));
                    return Err(error);
                }
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
        if r.subscription_type == SubscriptionType::Cyclic {
            return Err(DeviceGatewayError::unsupported(
                "MQTT has no cyclic subscription: a broker sends each message (DrtSubscribe or DrtEvent)",
            ));
        }
        let config = Settings::parse(&r)?;
        let mut session = Session::new(&r, &config)?;
        let topics = topics(&r)?;
        let pending = subscribe_topics(&mut session, &topics, config.qos, deadline).await?;
        // MQTT 5 asks the broker to send no more than receive-maximum unacknowledged messages; the margin is for what
        // arrives while subscribing. MQTT 3.1.1 has no such limit, so a full window pauses polling: a pause past the
        // keepalive closes the connection, and the session delivers the unacknowledged messages again.
        let limit = usize::from(config.receive_maximum) + if config.version == 5 { 64 } else { 0 };
        let shutdown = self.shutdown.clone();
        let mut durable = config
            .durable
            .then(|| Durable::start(r.clone(), sink.clone(), session.acker(), limit));
        Ok(tokio::spawn(async move {
            for message in pending {
                if !forward(&mut durable, &cancel, &r, sink.as_ref(), message, false).await {
                    break;
                }
            }
            loop {
                if config.version == 3 {
                    if let Some(durable) = durable.as_mut() {
                        if !durable.reserve(&cancel).await {
                            break;
                        }
                    }
                }
                let event = tokio::select! { biased; _ = cancel.cancelled() => break, event = session.poll() => event };
                match event {
                    Ok(Event::Message(topic, bytes, ack)) => {
                        if !forward(&mut durable, &cancel, &r, sink.as_ref(), (topic, bytes, ack), false).await {
                            break;
                        }
                    }
                    Ok(Event::Connected) => {
                        // Re-subscribe even if the broker lost its persistent session. Do not replay any command writes.
                        let restored = tokio::select! { biased;
                            _ = cancel.cancelled() => break,
                            result = subscribe_topics(&mut session, &topics, config.qos, r.deadline()) => result,
                        };
                        match restored {
                            Ok(pending) => {
                                for message in pending {
                                    if !forward(&mut durable, &cancel, &r, sink.as_ref(), message, true).await {
                                        break;
                                    }
                                }
                            }
                            Err(error) => {
                                tracing::error!(request=%r.name,error=%error,"MQTT subscription could not be restored");
                                break;
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(request=%r.name,error=%error,"MQTT subscription disconnected");
                        if !config.reconnect {
                            break;
                        }
                        tokio::select! { _ = cancel.cancelled() => break, _ = tokio::time::sleep(config.reconnect_delay) => {} }
                    }
                }
            }
            if let Some(durable) = durable.take() {
                // Send the acknowledgements of what the host has stored, polling so they go out. Messages arriving
                // meanwhile are not delivered and stay in the broker.
                let mut acknowledged = durable.finish();
                let drain = async {
                    loop {
                        tokio::select! { _ = &mut acknowledged => break, _ = session.poll() => {} }
                    }
                };
                if tokio::time::timeout(std::time::Duration::from_secs(10), drain)
                    .await
                    .is_err()
                {
                    tracing::warn!(request=%r.name,"MQTT stopped before every stored message was acknowledged; the broker delivers them again");
                }
            }
            if shutdown.load(Ordering::Acquire) {
                // Keep the subscription: the persistent session collects in the broker until the gateway is back.
                let leave = async {
                    session.disconnect().await?;
                    loop {
                        session.poll().await?;
                    }
                    #[allow(unreachable_code)]
                    Ok::<(), DeviceGatewayError>(())
                };
                let _ = tokio::time::timeout(std::time::Duration::from_secs(1), leave).await;
            } else if cancel.is_cancelled() {
                // A persistent session must not keep collecting messages after a successful explicit stop.
                let cleanup = async {
                    for topic in &topics {
                        session.unsubscribe(topic).await?;
                        loop {
                            match session.poll().await? {
                                Event::Unsubscribed(true) => break,
                                Event::Unsubscribed(false) => {
                                    return Err(DeviceGatewayError::transport("MQTT broker rejected unsubscribe"))
                                }
                                _ => {}
                            }
                        }
                    }
                    Ok::<(), DeviceGatewayError>(())
                };
                match tokio::time::timeout(std::time::Duration::from_secs(1), cleanup).await {
                    Ok(Ok(())) => {}
                    _ => {
                        tracing::warn!(request=%r.name,"MQTT stopped locally; broker subscription cleanup could not be confirmed")
                    }
                }
            }
        }))
    }
}
