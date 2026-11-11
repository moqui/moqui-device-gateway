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
#![cfg(feature = "ethercat")]
mod common;
use moqui_device_gateway::{
    backends::ethercat::{decode, encode, interface, Address, Area},
    *,
};
use std::sync::Arc;

#[test]
fn addresses_name_the_subdevice_the_byte_and_the_type() {
    let a = Address::parse("input:2:4:UINT").unwrap();
    assert_eq!(
        (a.area, a.subdevice, a.byte, a.bit, a.ty),
        (Area::Input, 2, 4, None, PhysicalType::U16)
    );
    let b = Address::parse("output:0:1.7:BOOL").unwrap();
    assert_eq!((b.area, b.byte, b.bit), (Area::Output, 1, Some(7)));
    for bad in [
        "input:0:1:BOOL",
        "input:0:1.2:UINT",
        "output:0:1.8:BOOL",
        "memory:0:0:UINT",
        "input:0:0:STRING",
    ] {
        assert!(Address::parse(bad).is_err(), "{bad}");
    }
    assert_eq!(interface("ethercat://enp3s0").unwrap(), "enp3s0");
    assert!(interface("ethercat://").is_err());
}

#[test]
fn process_data_is_little_endian_and_bits_leave_their_neighbours() {
    let mut image = vec![0u8; 8];
    encode(
        &mut image,
        &Address::parse("output:0:0:INT").unwrap(),
        &FieldValue::Signed(-2),
    )
    .unwrap();
    assert_eq!(&image[..2], &[0xfe, 0xff]);
    encode(
        &mut image,
        &Address::parse("output:0:2:REAL").unwrap(),
        &FieldValue::Float(1.0),
    )
    .unwrap();
    assert_eq!(&image[2..6], &1.0f32.to_le_bytes());
    image[6] = 0b1000_0001;
    encode(
        &mut image,
        &Address::parse("output:0:6.3:BOOL").unwrap(),
        &FieldValue::Bool(true),
    )
    .unwrap();
    assert_eq!(image[6], 0b1000_1001);
    assert_eq!(
        decode(&image, &Address::parse("output:0:0:INT").unwrap()).unwrap(),
        FieldValue::Signed(-2)
    );
    assert_eq!(
        decode(&image, &Address::parse("output:0:0:UINT").unwrap()).unwrap(),
        FieldValue::Unsigned(65534)
    );
    assert_eq!(
        decode(&image, &Address::parse("output:0:6.0:BOOL").unwrap()).unwrap(),
        FieldValue::Bool(true)
    );
    assert_eq!(
        decode(&image, &Address::parse("output:0:6.1:BOOL").unwrap()).unwrap(),
        FieldValue::Bool(false)
    );
    assert!(encode(
        &mut image,
        &Address::parse("output:0:0:USINT").unwrap(),
        &FieldValue::Unsigned(256)
    )
    .is_err());
    assert!(
        decode(&image, &Address::parse("input:0:6:UDINT").unwrap()).is_err(),
        "beyond the image"
    );
}

/// An interface that cannot be opened is an error, and the next request tries again.
#[tokio::test]
async fn an_interface_that_cannot_be_opened_fails_each_request() {
    let f = Arc::new(DeviceGatewayFacade::new());
    let mut r = common::request(Protocol::EtherCat, "ethercat://no-such-nic0".into(), Operation::Read);
    r.items[0].address = "input:0:0:UINT".into();
    r.items[0].physical_type = PhysicalType::U16;
    let r = Arc::new(r);
    for _ in 0..2 {
        let mut out = RequestOutput::default();
        let error = f
            .execute(r.clone(), &RequestInput::default(), &mut out, r.deadline(), None)
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Transport, "{}", error.message);
        assert!(error.message.contains("no-such-nic0"), "{}", error.message);
    }
    f.close().await;
}
