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

#![cfg(feature = "mqtt")]
mod common;
use moqui_device_gateway::*;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn packet(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let header = stream.read_u8().await.unwrap();
    let mut length = 0usize;
    let mut multiplier = 1;
    loop {
        let n = stream.read_u8().await.unwrap();
        length += (n as usize & 127) * multiplier;
        if n < 128 {
            break;
        }
        multiplier *= 128;
        assert!(multiplier <= 128 * 128 * 128);
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.unwrap();
    (header, body)
}

async fn send(stream: &mut TcpStream, header: u8, body: &[u8]) {
    let mut packet = vec![header];
    let mut n = body.len();
    loop {
        let mut byte = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            byte |= 128;
        }
        packet.push(byte);
        if n == 0 {
            break;
        }
    }
    packet.extend(body);
    stream.write_all(&packet).await.unwrap();
}

async fn broker(write: bool, reject: bool) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ep = format!("mqtt://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        s.set_nodelay(true).unwrap();
        let (header, body) = packet(&mut s).await;
        assert_eq!(header, 0x10);
        assert_eq!(&body[..7], &[0, 4, b'M', b'Q', b'T', b'T', 5]);
        send(&mut s, 0x20, &[0, 0, 0]).await;
        let (header, body) = packet(&mut s).await;
        if write {
            assert_eq!(header & 0xf6, 0x32);
            let len = u16::from_be_bytes([body[0], body[1]]) as usize;
            assert_eq!(&body[2..2 + len], b"/test/topic");
            let id = &body[2 + len..4 + len];
            assert_eq!(body[4 + len], 0);
            let value: serde_json::Value = serde_json::from_slice(&body[5 + len..]).unwrap();
            assert_eq!(value["parameterId"], "test-param");
            assert_eq!(value["numericValue"].as_u64(), Some(u64::MAX));
            assert_eq!(value["temperature"].as_u64(), Some(u64::MAX));
            if reject {
                send(&mut s, 0x40, &[id[0], id[1], 0x87, 0]).await;
            } else {
                send(&mut s, 0x40, id).await;
            }
        } else {
            assert_eq!(header, 0x82);
            let id = &body[..2];
            assert_eq!(body[2], 0);
            let topic = b"/test/topic";
            let mut publish = vec![0, topic.len() as u8];
            publish.extend(topic);
            publish.push(0);
            publish.extend(br#"{"temperature":18446744073709551615}"#);
            // Retained data can arrive while subscription setup is still in progress.
            send(&mut s, 0x30, &publish).await;
            send(&mut s, 0x90, &[id[0], id[1], 0, 1]).await;
        }
        let _ = tokio::time::timeout(Duration::from_millis(500), s.read_u8()).await;
    });
    (ep, task)
}

fn request(endpoint: String, operation: Operation) -> Arc<PreparedDeviceRequest> {
    let mut r = common::request(Protocol::Mqtt, endpoint, operation);
    r.items[0].address = "/test/topic".into();
    Arc::new(r)
}

#[tokio::test]
async fn qos1_publication_preserves_gateway_envelope() {
    let (ep, server) = broker(true, false).await;
    let r = request(ep, Operation::Write);
    let f = Arc::new(DeviceGatewayFacade::new());
    let input = RequestInput {
        values: std::collections::BTreeMap::from([("test-param".into(), FieldValue::Unsigned(u64::MAX))]),
        ..Default::default()
    };
    let mut out = RequestOutput::default();
    assert_eq!(
        f.execute(r.clone(), &input, &mut out, r.deadline(), None)
            .await
            .unwrap(),
        ExecutionOutcome::Completed
    );
    server.await.unwrap();
    f.close().await;
}

#[tokio::test]
async fn rejected_puback_cannot_report_success() {
    let (ep, server) = broker(true, true).await;
    let r = request(ep, Operation::Write);
    let f = Arc::new(DeviceGatewayFacade::new());
    let input = RequestInput {
        values: std::collections::BTreeMap::from([("test-param".into(), FieldValue::Unsigned(u64::MAX))]),
        ..Default::default()
    };
    let mut out = RequestOutput::default();
    assert!(f
        .execute(r.clone(), &input, &mut out, r.deadline(), None)
        .await
        .is_err());
    assert_eq!(out.items[0].status, ItemStatus::Failed);
    server.await.unwrap();
    f.close().await;
}

#[tokio::test]
async fn a_topic_cannot_be_read_polled_or_subscribed_cyclically() {
    let f = Arc::new(DeviceGatewayFacade::new());
    let sink: Arc<dyn SampleSink> = Arc::new(HeldSink::default());
    for (operation, subscription_type) in [
        (Operation::Read, SubscriptionType::ChangeOfState),
        (Operation::Poll, SubscriptionType::ChangeOfState),
        (Operation::Subscribe, SubscriptionType::Cyclic),
    ] {
        let mut r = common::request(Protocol::Mqtt, "mqtt://127.0.0.1:9".into(), operation);
        r.subscription_type = subscription_type;
        let r = Arc::new(r);
        let mut out = RequestOutput::default();
        let error = f
            .execute(
                r.clone(),
                &RequestInput::default(),
                &mut out,
                r.deadline(),
                Some(sink.clone()),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.kind,
            ErrorKind::Unsupported,
            "{operation:?} {subscription_type:?}"
        );
    }
    f.close().await;
}

#[tokio::test]
async fn payload_without_items_is_one_publication_on_the_request_topic() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ep = format!("mqtt://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let (header, _) = packet(&mut s).await;
        assert_eq!(header, 0x10);
        send(&mut s, 0x20, &[0, 0, 0]).await;
        let (header, body) = packet(&mut s).await;
        assert_eq!(header & 0xf6, 0x32);
        let len = u16::from_be_bytes([body[0], body[1]]) as usize;
        assert_eq!(&body[2..2 + len], b"moqui/robot/arm1/trajectory");
        let id = [body[2 + len], body[3 + len]];
        assert_eq!(body[4 + len], 0);
        let message: serde_json::Value = serde_json::from_slice(&body[5 + len..]).unwrap();
        assert_eq!(message["axes"]["X"][1], 860);
        send(&mut s, 0x40, &id).await;
        let _ = tokio::time::timeout(Duration::from_millis(500), s.read_u8()).await;
    });
    let mut r = common::request(Protocol::Mqtt, ep, Operation::Write);
    r.items.clear();
    r.query = Some("moqui/robot/arm1/trajectory".into());
    let r = Arc::new(r);
    let f = Arc::new(DeviceGatewayFacade::new());
    let input = RequestInput {
        payload: Some(br#"{"waypointCount":2,"axes":{"X":[850,860]}}"#.to_vec()),
        ..Default::default()
    };
    let mut out = RequestOutput::default();
    assert_eq!(
        f.execute(r.clone(), &input, &mut out, r.deadline(), None)
            .await
            .unwrap(),
        ExecutionOutcome::Completed
    );
    server.await.unwrap();
    f.close().await;
}

/// A host that keeps every receipt until the test says it stored the batch, or gave it up.
#[derive(Default)]
struct HeldSink {
    done: std::sync::Mutex<Vec<tokio::sync::oneshot::Sender<()>>>,
    delivered: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl SampleSink for HeldSink {
    fn emit(&self, _: SampleBatch) -> Result<(), DeviceGatewayError> {
        panic!("a durable subscription must deliver, not emit");
    }

    async fn deliver(&self, batch: SampleBatch) -> Result<Receipt, DeviceGatewayError> {
        assert_eq!(batch.samples.len(), 1);
        let (done, receipt) = Receipt::pending();
        self.done.lock().unwrap().push(done);
        self.delivered.notify_one();
        Ok(receipt)
    }
}

/// A QoS 1 message of a durable subscription is acknowledged once the host stored it, never on receipt; one the host
/// gives up is never acknowledged, so the broker keeps it.
#[tokio::test]
async fn durable_subscription_acknowledges_only_stored_messages() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ep = format!("mqtt://{}", listener.local_addr().unwrap());
    let (acked_tx, mut acked) = tokio::sync::mpsc::unbounded_channel::<[u8; 2]>();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        s.set_nodelay(true).unwrap();
        let (header, body) = packet(&mut s).await;
        assert_eq!(header, 0x10);
        assert_eq!(&body[..7], &[0, 4, b'M', b'Q', b'T', b'T', 5]);
        assert_eq!(body[7] & 0x02, 0, "a durable subscription needs a persistent session");
        assert!(
            body.windows(3).any(|w| w == [0x21, 0x01, 0x00]),
            "CONNECT must announce receive-maximum 256"
        );
        send(&mut s, 0x20, &[0, 0, 0]).await;
        let (header, body) = packet(&mut s).await;
        assert_eq!(header, 0x82);
        send(&mut s, 0x90, &[body[0], body[1], 0, 1]).await;
        for id in [1u8, 2] {
            let topic = b"/test/topic";
            let mut publish = vec![0, topic.len() as u8];
            publish.extend(topic);
            publish.extend([0, id, 0]);
            publish.extend(br#"{"temperature":21}"#);
            send(&mut s, 0x32, &publish).await;
        }
        loop {
            // The client closes the connection after its unsubscribe.
            let mut first = [0u8; 1];
            match tokio::time::timeout(Duration::from_secs(3), s.peek(&mut first)).await {
                Ok(Ok(1..)) => {}
                _ => break,
            }
            match packet(&mut s).await {
                (0x40, body) => acked_tx.send([body[0], body[1]]).unwrap(),
                (0xa2, body) => send(&mut s, 0xb0, &[body[0], body[1], 0, 0]).await,
                _ => {}
            }
        }
    });
    let mut r = common::request(Protocol::Mqtt, ep, Operation::Subscribe);
    r.items[0].address = "/test/topic".into();
    let r = Arc::new(r);
    let f = Arc::new(DeviceGatewayFacade::new());
    let sink = Arc::new(HeldSink::default());
    let mut out = RequestOutput::default();
    f.execute(
        r.clone(),
        &RequestInput::default(),
        &mut out,
        r.deadline(),
        Some(sink.clone()),
    )
    .await
    .unwrap();
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(2), sink.delivered.notified())
            .await
            .expect("message delivered to the host");
    }
    // Received and delivered, but not stored: the broker must still hold both.
    assert!(tokio::time::timeout(Duration::from_millis(300), acked.recv())
        .await
        .is_err());
    let (first, second) = {
        let mut done = sink.done.lock().unwrap();
        (done.remove(0), done.remove(0))
    };
    first.send(()).unwrap();
    let id = tokio::time::timeout(Duration::from_secs(2), acked.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(id, [0, 1]);
    // The host gives up the second (a shutdown before the database answered): no acknowledgement.
    drop(second);
    assert!(tokio::time::timeout(Duration::from_millis(300), acked.recv())
        .await
        .is_err());
    f.close().await;
    server.await.unwrap();
}
