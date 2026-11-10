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

mod common;
#[tokio::test]
async fn concurrent_pollers_have_independent_cancellation_and_shared_shutdown() {
    let reader = Arc::new(Reader(AtomicUsize::new(0)));
    let mut facade = DeviceGatewayFacade::new();
    facade.install(Protocol::ModbusTcp, reader.clone());
    let facade = Arc::new(facade);
    let mut first = common::request(Protocol::ModbusTcp, "localhost:502".into(), Operation::Poll);
    first.name = "first-poller".into();
    let mut second = first.clone();
    second.name = "second-poller".into();
    let start = |request: PreparedDeviceRequest| {
        let facade = facade.clone();
        async move {
            let request = Arc::new(request);
            facade
                .execute(
                    request.clone(),
                    &RequestInput::default(),
                    &mut RequestOutput::default(),
                    request.deadline(),
                    Some(Arc::new(Sink)),
                )
                .await
        }
    };
    let (a, b) = tokio::join!(start(first.clone()), start(second));
    assert_eq!(a.unwrap(), ExecutionOutcome::Active);
    assert_eq!(b.unwrap(), ExecutionOutcome::Active);
    assert_eq!(facade.subscription_names().await.0.len(), 2);
    let mut stop = first;
    stop.name = "stop-first".into();
    stop.operation = Operation::Unsubscribe;
    stop.parent_request = Some("first-poller".into());
    let stop = Arc::new(stop);
    facade
        .execute(
            stop.clone(),
            &RequestInput::default(),
            &mut RequestOutput::default(),
            stop.deadline(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(facade.subscription_names().await.0, vec!["second-poller"]);
    let before = reader.0.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(reader.0.load(Ordering::SeqCst) > before);
    facade.close().await;
    let closed = reader.0.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(reader.0.load(Ordering::SeqCst), closed);
}

use async_trait::async_trait;
use moqui_device_gateway::*;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
struct Reader(AtomicUsize);
#[async_trait]
impl DeviceFacade for Reader {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            read: true,
            write: false,
            subscribe: false,
        }
    }
    async fn execute(
        &self,
        _: &PreparedDeviceRequest,
        _: &RequestInput,
        out: &mut RequestOutput,
        _: Instant,
    ) -> Result<(), DeviceGatewayError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        out.items
            .push(ItemResult::value("test-param", FieldValue::Unsigned(42)));
        Ok(())
    }
}
struct Sink;
impl SampleSink for Sink {
    fn emit(&self, _: SampleBatch) -> Result<(), DeviceGatewayError> {
        Ok(())
    }
}
#[tokio::test]
async fn ownership_cancellation_and_shutdown() {
    let reader = Arc::new(Reader(AtomicUsize::new(0)));
    let mut facade = DeviceGatewayFacade::new();
    facade.install(Protocol::ModbusTcp, reader.clone());
    let facade = Arc::new(facade);
    let r = Arc::new(common::request(
        Protocol::ModbusTcp,
        "localhost:502".into(),
        Operation::Poll,
    ));
    let mut out = RequestOutput::default();
    assert_eq!(
        facade
            .execute(
                r.clone(),
                &RequestInput::default(),
                &mut out,
                r.deadline(),
                Some(Arc::new(Sink))
            )
            .await
            .unwrap(),
        ExecutionOutcome::Active
    );
    assert_eq!(
        facade
            .execute(
                r.clone(),
                &RequestInput::default(),
                &mut out,
                r.deadline(),
                Some(Arc::new(Sink))
            )
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Busy
    );
    let mut stop = (*r).clone();
    stop.name = "stop".into();
    stop.operation = Operation::Unsubscribe;
    stop.parent_request = Some(r.name.clone());
    stop.device_id = "wrong-device".into();
    let stop = Arc::new(stop);
    assert_eq!(
        facade
            .execute(stop.clone(), &RequestInput::default(), &mut out, stop.deadline(), None)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Invalid
    );
    let mut valid = (*stop).clone();
    valid.device_id = r.device_id.clone();
    let valid = Arc::new(valid);
    facade
        .execute(
            valid.clone(),
            &RequestInput::default(),
            &mut out,
            valid.deadline(),
            None,
        )
        .await
        .unwrap();
    let count = reader.0.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(reader.0.load(Ordering::SeqCst), count);
    facade.close().await;
    assert_eq!(
        facade
            .execute(r.clone(), &RequestInput::default(), &mut out, r.deadline(), None)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Closed
    );
}
#[tokio::test]
async fn event_subscription_does_not_silently_become_polling() {
    let reader = Arc::new(Reader(AtomicUsize::new(0)));
    let mut f = DeviceGatewayFacade::new();
    f.install(Protocol::ModbusTcp, reader);
    let f = Arc::new(f);
    let r = Arc::new(common::request(
        Protocol::ModbusTcp,
        "localhost:502".into(),
        Operation::Subscribe,
    ));
    let e = f
        .execute(
            r.clone(),
            &RequestInput::default(),
            &mut RequestOutput::default(),
            r.deadline(),
            Some(Arc::new(Sink)),
        )
        .await
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Unsupported);
}
struct PartialReader;
#[async_trait]
impl DeviceFacade for PartialReader {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            read: true,
            write: false,
            subscribe: false,
        }
    }
    async fn execute(
        &self,
        _: &PreparedDeviceRequest,
        _: &RequestInput,
        out: &mut RequestOutput,
        _: Instant,
    ) -> Result<(), DeviceGatewayError> {
        out.items
            .push(ItemResult::value("test-param", FieldValue::Unsigned(10)));
        Err(DeviceGatewayError::transport("second item failed"))
    }
}
#[tokio::test]
async fn partial_read_decodes_successes_and_marks_missing_items() {
    let mut r = common::request(Protocol::ModbusTcp, "localhost:502".into(), Operation::Read);
    r.items[0].codec.scale = 2u32.into();
    let mut second = r.items[0].clone();
    second.parameter_id = "second-param".into();
    r.items.push(second);
    let r = Arc::new(r);
    let mut f = DeviceGatewayFacade::new();
    f.install(Protocol::ModbusTcp, Arc::new(PartialReader));
    let f = Arc::new(f);
    let mut out = RequestOutput::default();
    assert!(f
        .execute(r.clone(), &RequestInput::default(), &mut out, r.deadline(), None)
        .await
        .is_err());
    assert_eq!(out.items[0].value, Some(FieldValue::Decimal(20u32.into())));
    assert_eq!(out.items[1].status, ItemStatus::Failed);
    assert_eq!(out.outcome(), ExecutionOutcome::Partial);
    f.close().await;
}
