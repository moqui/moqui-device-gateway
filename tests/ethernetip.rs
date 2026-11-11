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
#![cfg(feature = "ethernetip")]
mod common;
use moqui_device_gateway::{
    backends::ethernetip::{from_plc, socket_address, to_plc},
    *,
};
use rust_ethernet_ip::PlcValue;
use std::{sync::Arc, time::Duration};

#[test]
fn endpoints_default_to_the_ethernet_ip_port() {
    assert_eq!(socket_address("logix://10.0.0.5").unwrap(), "10.0.0.5:44818");
    assert_eq!(socket_address("eip://plc.local:2222").unwrap(), "plc.local:2222");
    assert!(socket_address("logix://10.0.0.5/1/0").is_err());
}

#[test]
fn values_convert_by_the_physical_type_and_refuse_overflow() {
    assert!(matches!(
        to_plc(&FieldValue::Signed(-5), PhysicalType::I32).unwrap(),
        PlcValue::Dint(-5)
    ));
    assert!(matches!(
        to_plc(&FieldValue::Unsigned(7), PhysicalType::U16).unwrap(),
        PlcValue::Uint(7)
    ));
    assert!(matches!(to_plc(&FieldValue::Float(1.5), PhysicalType::F32).unwrap(), PlcValue::Real(v) if v == 1.5));
    assert!(matches!(
        to_plc(&FieldValue::Bool(true), PhysicalType::Bool).unwrap(),
        PlcValue::Bool(true)
    ));
    assert!(to_plc(&FieldValue::Signed(40_000), PhysicalType::I16).is_err());
    assert!(to_plc(&FieldValue::Signed(-1), PhysicalType::U32).is_err());
    assert!(to_plc(&FieldValue::Text("x".into()), PhysicalType::I32).is_err());
    assert_eq!(from_plc(PlcValue::Int(-3)).unwrap(), FieldValue::Signed(-3));
    assert_eq!(from_plc(PlcValue::Lreal(2.25)).unwrap(), FieldValue::Float(2.25));
}

fn request(endpoint: &str, operation: Operation, items: &[(&str, &str, PhysicalType)]) -> PreparedDeviceRequest {
    let mut r = common::request(Protocol::EthernetIp, endpoint.into(), operation);
    let template = r.items[0].clone();
    r.items = items
        .iter()
        .map(|(id, tag, ty)| Item {
            parameter_id: (*id).into(),
            address: (*tag).into(),
            physical_type: *ty,
            ..template.clone()
        })
        .collect();
    r.timeout = Duration::from_secs(5);
    r
}

/// Against a Logix simulator: `python -m cpppo.server.enip --address 127.0.0.1:44818 Speed=DINT Temp=REAL Level=INT`,
/// with `GATEWAY_EIP_ENDPOINT=logix://127.0.0.1:44818`.
#[tokio::test]
async fn tags_are_written_and_read_back() {
    let Ok(endpoint) = std::env::var("GATEWAY_EIP_ENDPOINT") else {
        eprintln!("GATEWAY_EIP_ENDPOINT is not set: skipped");
        return;
    };
    let items = [
        ("speed", "Speed", PhysicalType::I32),
        ("temp", "Temp", PhysicalType::F32),
        ("level", "Level", PhysicalType::I16),
    ];
    let f = Arc::new(DeviceGatewayFacade::new());
    let write = Arc::new(request(&endpoint, Operation::Write, &items));
    let mut input = RequestInput::default();
    input.values.insert("speed".into(), FieldValue::Signed(-1234));
    input.values.insert("temp".into(), FieldValue::Float(21.5));
    input.values.insert("level".into(), FieldValue::Signed(17));
    let mut out = RequestOutput::default();
    f.execute(write.clone(), &input, &mut out, write.deadline(), None)
        .await
        .unwrap();
    let read = Arc::new(request(&endpoint, Operation::Read, &items));
    let mut out = RequestOutput::default();
    f.execute(read.clone(), &RequestInput::default(), &mut out, read.deadline(), None)
        .await
        .unwrap();
    let value = |id: &str| {
        out.items
            .iter()
            .find(|i| i.parameter_id == id)
            .and_then(|i| i.value.clone())
            .and_then(|v| v.decimal().ok())
            .map(|d| d.to_string())
    };
    assert_eq!(value("speed").as_deref(), Some("-1234"));
    assert_eq!(value("temp").as_deref(), Some("21.5"));
    assert_eq!(value("level").as_deref(), Some("17"));
    // An unknown tag fails its item.
    let missing = Arc::new(request(
        &endpoint,
        Operation::Read,
        &[("x", "NoSuchTag", PhysicalType::I32)],
    ));
    let mut out = RequestOutput::default();
    assert!(f
        .execute(
            missing.clone(),
            &RequestInput::default(),
            &mut out,
            missing.deadline(),
            None
        )
        .await
        .is_err());
    assert_eq!(out.items[0].status, ItemStatus::Failed);
    f.close().await;
}
