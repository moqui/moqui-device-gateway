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

//! The OPC UA server of the OPC UA specs (test/DeviceGateway.spec.js, gateway_opcua_test): the Milo test server of
//! the former gateway tests, at opc.tcp://127.0.0.1:12686/milo with the writable nodes
//! ns=2;s=Demo.Dynamic.Scalar.Double and ns=2;s=Demo.Dynamic.Scalar.Boolean that moqui-device's DeviceTestData.xml
//! requests use. Anonymous, no security.
//!
//!     cargo run --example opcua_test_server [port] [pki-directory]

use opcua::{
    nodes::{AccessLevel, Variable},
    server::{
        diagnostics::NamespaceMetadata,
        node_manager::memory::{simple_node_manager, SimpleNodeManager},
        ServerBuilder, ServerEndpoint, ANONYMOUS_USER_TOKEN_ID,
    },
    types::{NodeId, ObjectId, Variant},
};

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let port: u16 = args.next().and_then(|p| p.parse().ok()).unwrap_or(12686);
    let pki = args.next().unwrap_or_else(|| "target/opcua-test-server-pki".into());
    let (server, handle) = ServerBuilder::new()
        .application_name("moqui-device-gateway OPC UA test server")
        .application_uri("urn:moqui-device-gateway-test-server")
        .host("127.0.0.1")
        .port(port)
        .pki_dir(pki)
        .create_sample_keypair(true)
        .trust_client_certs(true)
        .add_endpoint(
            "milo",
            ServerEndpoint::new_none("/milo", &[ANONYMOUS_USER_TOKEN_ID.to_string()]),
        )
        .discovery_urls(vec!["/milo".to_owned()])
        .with_node_manager(simple_node_manager(
            NamespaceMetadata {
                namespace_uri: "urn:moqui-device-gateway-test".to_owned(),
                ..Default::default()
            },
            "demo",
        ))
        .build()
        .expect("OPC UA test server configuration");
    let namespace = handle
        .get_namespace_index("urn:moqui-device-gateway-test")
        .expect("test namespace");
    assert_eq!(namespace, 2, "the specs address ns=2");
    let manager = handle
        .node_managers()
        .get_of_type::<SimpleNodeManager>()
        .expect("simple node manager");
    {
        let mut space = manager.address_space().write();
        let writable = AccessLevel::CURRENT_READ | AccessLevel::CURRENT_WRITE;
        let mut double = Variable::new(
            &NodeId::new(namespace, "Demo.Dynamic.Scalar.Double"),
            "Double",
            "Double",
            Variant::Double(21.5),
        );
        double.set_access_level(writable);
        double.set_user_access_level(writable);
        let mut boolean = Variable::new(
            &NodeId::new(namespace, "Demo.Dynamic.Scalar.Boolean"),
            "Boolean",
            "Boolean",
            Variant::Boolean(false),
        );
        boolean.set_access_level(writable);
        boolean.set_user_access_level(writable);
        space.add_variables(vec![double, boolean], &ObjectId::ObjectsFolder.into());
    }
    println!("OPC UA test server on opc.tcp://127.0.0.1:{port}/milo");
    server.run().await.expect("OPC UA test server");
}
