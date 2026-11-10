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

//! Protocol contract shared by Moqui and independent Rust hosts. No entity or service facade crosses this boundary.
use crate::model::*;
use async_trait::async_trait;
use std::{sync::Arc, time::Instant};
use tokio_util::sync::CancellationToken;

/// Resolves when the host is done with a delivered batch: stored, or rejected for good.
pub struct Receipt(Option<tokio::sync::oneshot::Receiver<()>>);

impl Receipt {
    /// A batch the host was done with on delivery.
    pub fn done() -> Self {
        Self(None)
    }

    /// A batch the host finishes with when `done` sends, or gives up on when `done` is dropped.
    pub fn pending() -> (tokio::sync::oneshot::Sender<()>, Self) {
        let (done, receipt) = tokio::sync::oneshot::channel();
        (done, Self(Some(receipt)))
    }

    /// True when the source may acknowledge the batch; false when the host gave it up (a shutdown) and the source is
    /// to deliver it again later.
    pub async fn finished(self) -> bool {
        match self.0 {
            None => true,
            Some(receipt) => receipt.await.is_ok(),
        }
    }
}

#[async_trait]
pub trait SampleSink: Send + Sync {
    /// Must not block a protocol callback. A full queue is an observable failure.
    fn emit(&self, batch: SampleBatch) -> Result<(), DeviceGatewayError>;

    /// For a source that keeps a batch until the host is done with it, as an MQTT persistent session keeps an
    /// unacknowledged message in the broker: waits for room instead of rejecting, and the receipt tells when the
    /// source may acknowledge. The default is [`Self::emit`], done on delivery.
    async fn deliver(&self, batch: SampleBatch) -> Result<Receipt, DeviceGatewayError> {
        self.emit(batch).map(|()| Receipt::done())
    }
}

#[async_trait]
pub trait DeviceFacade: Send + Sync {
    /// The host is shutting down, before its subscriptions are cancelled: a subscription whose broker keeps a session
    /// for it ends without unsubscribing, so the session collects until the host is back. An explicit unsubscribe
    /// cancels without this.
    fn shutting_down(&self) {}

    fn capabilities(&self) -> Capabilities;
    async fn execute(
        &self,
        request: &PreparedDeviceRequest,
        input: &RequestInput,
        output: &mut RequestOutput,
        deadline: Instant,
    ) -> Result<(), DeviceGatewayError>;

    async fn subscribe(
        &self,
        _request: Arc<PreparedDeviceRequest>,
        _sink: Arc<dyn SampleSink>,
        _cancel: CancellationToken,
        _deadline: Instant,
    ) -> Result<tokio::task::JoinHandle<()>, DeviceGatewayError> {
        Err(DeviceGatewayError::unsupported(
            "This backend has no event subscription",
        ))
    }

    async fn close(&self) {}
}
