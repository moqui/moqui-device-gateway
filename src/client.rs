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

//! [`DeviceGatewayFacade`]: the one API of every protocol. It runs reads and writes on the backend of the connection,
//! keeps the subscriptions and polling tasks by request name, and converts between the physical values of the backends
//! and the logical values of the parameters.

pub use crate::device_facade::{DeviceFacade, Receipt, SampleSink};
use crate::model::*;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Instant, SystemTime},
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

struct Subscription {
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    request: Arc<PreparedDeviceRequest>,
}

pub struct DeviceGatewayFacade {
    clients: HashMap<ProtocolKey, Arc<dyn DeviceFacade>>,
    subscriptions: Mutex<HashMap<String, Subscription>>,
    lifecycle: Mutex<()>,
    closed: AtomicBool,
}

#[derive(Hash, Eq, PartialEq)]
struct ProtocolKey(u8);
impl From<Protocol> for ProtocolKey {
    fn from(p: Protocol) -> Self {
        Self(p as u8)
    }
}

impl Default for DeviceGatewayFacade {
    fn default() -> Self {
        Self::new()
    }
}

impl DeviceGatewayFacade {
    pub fn new() -> Self {
        let mut facade = Self {
            clients: HashMap::new(),
            subscriptions: Mutex::new(HashMap::new()),
            lifecycle: Mutex::new(()),
            closed: AtomicBool::new(false),
        };
        #[cfg(feature = "modbus")]
        facade.install(
            Protocol::ModbusTcp,
            Arc::new(crate::backends::modbus::ModbusDeviceClient::default()),
        );
        #[cfg(feature = "mqtt")]
        facade.install(
            Protocol::Mqtt,
            Arc::new(crate::backends::mqtt::MqttDeviceClient::default()),
        );
        #[cfg(feature = "opcua")]
        facade.install(
            Protocol::OpcUa,
            Arc::new(crate::backends::opcua::OpcUaDeviceClient::default()),
        );
        #[cfg(feature = "ethernetip")]
        facade.install(
            Protocol::EthernetIp,
            Arc::new(crate::backends::ethernetip::EthernetIpDeviceClient::default()),
        );
        #[cfg(feature = "ethercat")]
        facade.install(
            Protocol::EtherCat,
            Arc::new(crate::backends::ethercat::EtherCatDeviceClient::default()),
        );
        facade
    }

    pub fn install(&mut self, protocol: Protocol, client: Arc<dyn DeviceFacade>) {
        self.clients.insert(protocol.into(), client);
    }

    pub async fn execute(
        self: &Arc<Self>,
        request: Arc<PreparedDeviceRequest>,
        input: &RequestInput,
        output: &mut RequestOutput,
        deadline: Instant,
        sink: Option<Arc<dyn SampleSink>>,
    ) -> Result<ExecutionOutcome, DeviceGatewayError> {
        request.validate()?;
        *output = RequestOutput::default();
        if self.closed.load(Ordering::Acquire) {
            return Err(DeviceGatewayError::new(
                ErrorKind::Closed,
                "Device gateway is shutting down",
            ));
        }
        if deadline <= Instant::now() {
            return Err(DeviceGatewayError::timeout(false));
        }
        if request.operation == Operation::Unsubscribe {
            let _guard = tokio::time::timeout_at(deadline.into(), self.lifecycle.lock())
                .await
                .map_err(|_| DeviceGatewayError::timeout(false))?;
            let key = request
                .parent_request
                .as_ref()
                .ok_or_else(|| DeviceGatewayError::invalid("parentRequestName is required"))?;
            let mut subscriptions = self.subscriptions.lock().await;
            if subscriptions
                .get(key)
                .is_some_and(|s| s.request.device_id != request.device_id)
            {
                return Err(DeviceGatewayError::invalid(
                    "Subscription belongs to a different device",
                ));
            }
            let sub = subscriptions.remove(key);
            drop(subscriptions);
            if let Some(mut sub) = sub {
                sub.cancel.cancel();
                if tokio::time::timeout_at(deadline.into(), &mut sub.task).await.is_err() {
                    sub.task.abort();
                    let _ = sub.task.await;
                    return Err(DeviceGatewayError::timeout(false));
                }
            }
            return Ok(ExecutionOutcome::Completed);
        }
        let client = self
            .clients
            .get(&request.connection.protocol.into())
            .cloned()
            .ok_or_else(|| DeviceGatewayError::unsupported("Protocol is not compiled into this component"))?;
        let cap = client.capabilities();
        match request.operation {
            Operation::Read | Operation::Poll if !cap.read => {
                return Err(DeviceGatewayError::unsupported(format!(
                    "{:?} cannot read or poll: subscribe to its values",
                    request.connection.protocol
                )))
            }
            Operation::Write if !cap.write => {
                return Err(DeviceGatewayError::unsupported(format!(
                    "{:?} cannot write",
                    request.connection.protocol
                )))
            }
            Operation::Subscribe if !cap.subscribe => {
                return Err(DeviceGatewayError::unsupported(format!(
                    "{:?} cannot subscribe: poll a DrtRead with a pollingInterval",
                    request.connection.protocol
                )))
            }
            _ => {}
        }
        if matches!(request.operation, Operation::Poll | Operation::Subscribe) {
            let sink = sink.ok_or_else(|| DeviceGatewayError::invalid("Subscription requires a sample sink"))?;
            let _guard = tokio::time::timeout_at(deadline.into(), self.lifecycle.lock())
                .await
                .map_err(|_| DeviceGatewayError::timeout(false))?;
            let running = self
                .subscriptions
                .lock()
                .await
                .get(&request.name)
                .is_some_and(|existing| !existing.task.is_finished());
            // Starting a running subscription again changes nothing: a restoration can ask for every subscription
            // that should run. Unsubscribe before replacing one.
            if running {
                return Ok(ExecutionOutcome::Unchanged);
            }
            let cancel = CancellationToken::new();
            let task = if request.operation == Operation::Subscribe {
                client
                    .subscribe(
                        request.clone(),
                        Arc::new(DecodedSink {
                            request: request.clone(),
                            inner: sink,
                        }),
                        cancel.clone(),
                        deadline,
                    )
                    .await?
            } else {
                // One read first: polling is active only once the device answers.
                let mut probe = RequestOutput::default();
                self.read_write(&request, input, &mut probe, deadline, client.clone())
                    .await?;
                sink.emit(batch(&request, probe.items))?;
                let facade = Arc::downgrade(self);
                let req = request.clone();
                let stop = cancel.clone();
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval_at(
                        tokio::time::Instant::now() + req.polling_interval,
                        req.polling_interval,
                    );
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    // The log tells when polling starts and stops failing, not every failed cycle: a device that is
                    // off at a short interval would otherwise write many lines a second. Every failure still reaches
                    // the sink, which notifies the users of a lasting one.
                    let mut failing = false;
                    loop {
                        tokio::select! {
                            _ = stop.cancelled() => break,
                            _ = interval.tick() => {}
                        }
                        let Some(facade) = facade.upgrade() else {
                            break;
                        };
                        let mut out = RequestOutput::default();
                        let result = facade
                            .read_write(&req, &RequestInput::default(), &mut out, req.deadline(), client.clone())
                            .await;
                        let mut polled = batch(&req, out.items);
                        match result {
                            Ok(()) if failing => {
                                failing = false;
                                tracing::info!(request = %req.name, "Field polling reads again");
                            }
                            Ok(()) => {}
                            Err(error) => {
                                if !failing {
                                    failing = true;
                                    tracing::error!(
                                        request = %req.name,
                                        error = %error,
                                        "Field polling failed; it goes on at every interval"
                                    );
                                }
                                polled.errors.push(error.to_string());
                            }
                        }
                        if let Err(error) = sink.emit(polled) {
                            tracing::error!(request = %req.name, error = %error, "Field samples were not queued");
                        }
                    }
                })
            };
            self.subscriptions.lock().await.insert(
                request.name.clone(),
                Subscription {
                    cancel,
                    task,
                    request: request.clone(),
                },
            );
            output.subscription_id = Some(request.name.clone());
            return Ok(ExecutionOutcome::Active);
        }
        self.read_write(&request, input, output, deadline, client).await?;
        Ok(output.outcome())
    }

    async fn read_write(
        &self,
        request: &PreparedDeviceRequest,
        input: &RequestInput,
        output: &mut RequestOutput,
        deadline: Instant,
        client: Arc<dyn DeviceFacade>,
    ) -> Result<(), DeviceGatewayError> {
        let mut physical = input.clone();
        if request.operation == Operation::Write {
            for item in &request.items {
                let value = input
                    .values
                    .get(&item.parameter_id)
                    .ok_or_else(|| DeviceGatewayError::invalid("Write item has no logical value"))?;
                physical.values.insert(
                    item.parameter_id.clone(),
                    item.codec.encode(value, item.kind, item.physical_type)?,
                );
            }
        }
        let writing = request.operation == Operation::Write;
        let execution = tokio::time::timeout_at(deadline.into(), client.execute(request, &physical, output, deadline))
            .await
            .map_err(|_| DeviceGatewayError::timeout(writing))
            .and_then(|result| result);
        if request.operation != Operation::Write {
            decode_results(request, &mut output.items);
        }
        if let Err(error) = execution {
            for item in &request.items {
                if !output
                    .items
                    .iter()
                    .any(|result| result.parameter_id == item.parameter_id)
                {
                    output.items.push(ItemResult::error(&item.parameter_id, error.clone()));
                }
            }
            return Err(error);
        }
        Ok(())
    }

    /// The requests with a subscription or polling task: those whose task runs, and those whose task ended without an
    /// unsubscribe (a lost connection). An unsubscribed request is in neither list.
    pub async fn subscription_names(&self) -> (Vec<String>, Vec<String>) {
        let (mut active, mut dropped) = (Vec::new(), Vec::new());
        for (name, s) in self.subscriptions.lock().await.iter() {
            if s.task.is_finished() {
                dropped.push(name.clone());
            } else {
                active.push(name.clone());
            }
        }
        active.sort();
        dropped.sort();
        (active, dropped)
    }

    pub async fn close(&self) {
        let _guard = self.lifecycle.lock().await;
        self.closed.store(true, Ordering::Release);
        for client in self.clients.values() {
            client.shutting_down();
        }
        let subs = std::mem::take(&mut *self.subscriptions.lock().await);
        for sub in subs.values() {
            sub.cancel.cancel();
        }
        for (_, mut sub) in subs {
            // A durable subscription acknowledges what the host stored before it ends.
            if tokio::time::timeout(std::time::Duration::from_secs(12), &mut sub.task)
                .await
                .is_err()
            {
                sub.task.abort();
                let _ = sub.task.await;
            }
        }
        for client in self.clients.values() {
            if tokio::time::timeout(std::time::Duration::from_secs(5), client.close())
                .await
                .is_err()
            {
                tracing::error!("Device gateway backend did not close within five seconds");
            }
        }
    }
}

fn decode_results(request: &PreparedDeviceRequest, results: &mut [ItemResult]) {
    for result in results {
        if result.status != ItemStatus::Completed {
            continue;
        }
        if let (Some(item), Some(value)) = (
            request.items.iter().find(|i| i.parameter_id == result.parameter_id),
            result.value.as_ref(),
        ) {
            match item.codec.decode(value, item.kind) {
                Ok(value) => result.value = Some(value),
                Err(error) => {
                    result.status = ItemStatus::Failed;
                    result.error = Some(error.message);
                    result.value = None;
                }
            }
        }
    }
}

/// The samples of a read: the values read, and an error for each item that failed.
fn batch(request: &Arc<PreparedDeviceRequest>, items: Vec<ItemResult>) -> SampleBatch {
    SampleBatch {
        request_name: request.name.clone(),
        errors: items
            .iter()
            .filter(|i| matches!(i.status, ItemStatus::Failed | ItemStatus::Uncertain))
            .map(|i| {
                format!(
                    "{}: {}",
                    i.parameter_id,
                    i.error.as_deref().unwrap_or("Acquisition failed")
                )
            })
            .collect(),
        logging: request.logging,
        request: Some(request.clone()),
        samples: items
            .into_iter()
            .filter(|i| i.status == ItemStatus::Completed)
            .filter_map(|i| {
                i.value.map(|value| Sample {
                    device_id: request.device_id.clone(),
                    log_value_kind: None,
                    parameter_id: Some(i.parameter_id),
                    value,
                    source_time: i.source_time,
                    received_time: SystemTime::now(),
                    quality: i.quality,
                })
            })
            .collect(),
    }
}

/// The sink of a subscription: decodes the physical values of the backend to the logical values of the parameters, and
/// attaches the request to each batch.
struct DecodedSink {
    request: Arc<PreparedDeviceRequest>,
    inner: Arc<dyn SampleSink>,
}

#[async_trait::async_trait]
impl SampleSink for DecodedSink {
    fn emit(&self, value: SampleBatch) -> Result<(), DeviceGatewayError> {
        self.inner.emit(self.decode(value))
    }

    async fn deliver(&self, value: SampleBatch) -> Result<crate::device_facade::Receipt, DeviceGatewayError> {
        self.inner.deliver(self.decode(value)).await
    }
}

impl DecodedSink {
    fn decode(&self, mut value: SampleBatch) -> SampleBatch {
        value.request = Some(self.request.clone());
        if !self.request.plc_log {
            value.samples.retain_mut(|sample| {
                if let Some(item) = self
                    .request
                    .items
                    .iter()
                    .find(|i| Some(&i.parameter_id) == sample.parameter_id.as_ref())
                {
                    match item.codec.decode(&sample.value, item.kind) {
                        Ok(decoded) => sample.value = decoded,
                        Err(error) => {
                            value.errors.push(format!("{}: {}", item.parameter_id, error));
                            return false;
                        }
                    }
                }
                true
            });
        }
        value
    }
}
