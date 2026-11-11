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

//! The OPC UA backend against an in-process OPC UA server: the read, write and subscription cases of the former
//! gateway's OpcUaGatewayIntegrationTest (Milo server), here with async-opcua's server.

#![cfg(feature = "opcua")]
mod common;
use moqui_device_gateway::*;
use opcua::{
    nodes::Variable,
    server::{
        diagnostics::NamespaceMetadata,
        node_manager::memory::{simple_node_manager, SimpleNodeManager},
        ServerBuilder, ServerHandle,
    },
    types::{DataValue, NodeId, ObjectId, Variant},
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

const NAMESPACE: &str = "urn:moqui-device-gateway-test";

struct TestServer {
    handle: ServerHandle,
    namespace: u16,
    endpoint: String,
    _pki: tempfile::TempDir,
}

impl TestServer {
    async fn start() -> Self {
        let pki = tempfile::tempdir().unwrap();
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let (server, handle) = ServerBuilder::new_anonymous("moqui-device-gateway test")
            .application_uri("urn:moqui-device-gateway-test-server")
            .host("127.0.0.1")
            .port(port)
            .pki_dir(pki.path().join("server"))
            .create_sample_keypair(true)
            .trust_client_certs(true)
            .with_node_manager(simple_node_manager(
                NamespaceMetadata {
                    namespace_uri: NAMESPACE.to_owned(),
                    ..Default::default()
                },
                "test",
            ))
            .build()
            .unwrap();
        let namespace = handle.get_namespace_index(NAMESPACE).unwrap();
        let manager = handle.node_managers().get_of_type::<SimpleNodeManager>().unwrap();
        {
            let mut space = manager.address_space().write();
            let mut temperature = Variable::new(
                &NodeId::new(namespace, "Temperature"),
                "Temperature",
                "Temperature",
                21.5f64,
            );
            temperature.set_user_access_level(
                opcua::nodes::AccessLevel::CURRENT_READ | opcua::nodes::AccessLevel::CURRENT_WRITE,
            );
            temperature
                .set_access_level(opcua::nodes::AccessLevel::CURRENT_READ | opcua::nodes::AccessLevel::CURRENT_WRITE);
            space.add_variables(vec![temperature], &ObjectId::ObjectsFolder.into());
        }
        tokio::spawn(server.run());
        // the listener is up when a TCP connection succeeds
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Self {
            handle,
            namespace,
            endpoint: format!("opc.tcp://127.0.0.1:{port}/"),
            _pki: pki,
        }
    }

    fn request(&self, operation: Operation) -> Arc<PreparedDeviceRequest> {
        let mut r = common::request(Protocol::OpcUa, self.endpoint.clone(), operation);
        r.items[0].address = format!("ns={};s=Temperature", self.namespace);
        r.items[0].physical_type = PhysicalType::F64;
        r.connection
            .options
            .insert("pkiDir".into(), self._pki.path().join("client").display().to_string());
        r.timeout = Duration::from_secs(10);
        Arc::new(r)
    }

    fn set(&self, value: f64) {
        let manager = self.handle.node_managers().get_of_type::<SimpleNodeManager>().unwrap();
        manager
            .set_value(
                self.handle.subscriptions(),
                &NodeId::new(self.namespace, "Temperature"),
                None,
                DataValue::new_now(Variant::Double(value)),
            )
            .unwrap();
    }

    fn value(&self) -> Variant {
        let manager = self.handle.node_managers().get_of_type::<SimpleNodeManager>().unwrap();
        let space = manager.address_space().read();
        let node = space.find(&NodeId::new(self.namespace, "Temperature")).unwrap();
        match node {
            opcua::nodes::NodeType::Variable(v) => v
                .value(
                    opcua::types::TimestampsToReturn::Neither,
                    &opcua::types::NumericRange::None,
                    &opcua::types::DataEncoding::Binary,
                    0.0,
                )
                .value
                .unwrap(),
            _ => panic!("not a variable"),
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.cancel();
    }
}

struct VecSink(Mutex<Vec<SampleBatch>>);
impl SampleSink for VecSink {
    fn emit(&self, batch: SampleBatch) -> Result<(), DeviceGatewayError> {
        self.0.lock().unwrap().push(batch);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn one_shot_read_fetches_the_current_node_value() {
    let server = TestServer::start().await;
    let r = server.request(Operation::Read);
    let f = Arc::new(DeviceGatewayFacade::new());
    let mut out = RequestOutput::default();
    let outcome = f
        .execute(r.clone(), &RequestInput::default(), &mut out, r.deadline(), None)
        .await
        .unwrap();
    assert_eq!(outcome, ExecutionOutcome::Completed);
    assert_eq!(
        out.items[0].value.as_ref().unwrap().decimal().unwrap().to_string(),
        "21.5"
    );
    f.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn write_publishes_the_parameter_value_to_the_server_node() {
    let server = TestServer::start().await;
    let r = server.request(Operation::Write);
    let f = Arc::new(DeviceGatewayFacade::new());
    let input = RequestInput {
        values: std::collections::BTreeMap::from([(
            "test-param".into(),
            FieldValue::Decimal("33.25".parse().unwrap()),
        )]),
        ..Default::default()
    };
    let mut out = RequestOutput::default();
    let outcome = f
        .execute(r.clone(), &input, &mut out, r.deadline(), None)
        .await
        .unwrap();
    assert_eq!(outcome, ExecutionOutcome::Completed);
    assert_eq!(server.value(), Variant::Double(33.25));
    f.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn subscription_delivers_live_node_updates() {
    live_node_updates(Operation::Subscribe, SubscriptionType::ChangeOfState).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cyclic_subscription_delivers_live_node_updates() {
    live_node_updates(Operation::Subscribe, SubscriptionType::Cyclic).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn polling_delivers_live_node_updates() {
    live_node_updates(Operation::Poll, SubscriptionType::ChangeOfState).await;
}

async fn live_node_updates(operation: Operation, subscription_type: SubscriptionType) {
    let server = TestServer::start().await;
    let mut r = (*server.request(operation)).clone();
    r.subscription_type = subscription_type;
    r.polling_interval = Duration::from_millis(100);
    let r = Arc::new(r);
    let f = Arc::new(DeviceGatewayFacade::new());
    let sink = Arc::new(VecSink(Mutex::new(Vec::new())));
    let mut out = RequestOutput::default();
    let outcome = f
        .execute(
            r.clone(),
            &RequestInput::default(),
            &mut out,
            r.deadline(),
            Some(sink.clone()),
        )
        .await
        .unwrap();
    assert_eq!(outcome, ExecutionOutcome::Active);
    // let the initial value through, then change the node on the server
    tokio::time::sleep(Duration::from_millis(500)).await;
    server.set(44.75);
    let mut seen = false;
    for _ in 0..100 {
        seen = sink.0.lock().unwrap().iter().flat_map(|b| &b.samples).any(|s| {
            s.parameter_id.as_deref() == Some("test-param") && s.value.decimal().is_ok_and(|d| d.to_string() == "44.75")
        });
        if seen {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        seen,
        "{operation:?} {subscription_type:?} did not deliver the new node value"
    );
    f.close().await;
}
