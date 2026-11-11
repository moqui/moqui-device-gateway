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
use moqui_device_gateway::{codec::ValueCodec, payload, *};
use rust_decimal::Decimal;
use std::str::FromStr;
fn d(s: &str) -> Decimal {
    Decimal::from_str(s).unwrap()
}
#[test]
fn zero_bounds_scale_offset_and_inverse() {
    let codec = ValueCodec {
        physical_min: Some(d("0")),
        physical_max: Some(d("1000")),
        logical_min: Some(d("0")),
        logical_max: Some(d("100")),
        scale: d("0.5"),
        offset: d("2"),
        digits: Some(2),
        ..Default::default()
    };
    let logical = codec
        .decode(&FieldValue::Unsigned(500), ParameterKind::Numeric)
        .unwrap();
    assert_eq!(logical, FieldValue::Decimal(d("27")));
    assert_eq!(
        codec
            .encode(&logical, ParameterKind::Numeric, PhysicalType::U16)
            .unwrap(),
        FieldValue::Unsigned(500)
    );
}
#[test]
fn a_write_beyond_the_parameter_bounds_is_refused() {
    let codec = ValueCodec {
        logical_min: Some(d("-10")),
        logical_max: Some(d("100")),
        ..Default::default()
    };
    for value in ["100.01", "-10.5"] {
        let error = codec
            .encode(
                &FieldValue::Decimal(d(value)),
                ParameterKind::Numeric,
                PhysicalType::F64,
            )
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Invalid, "{value}");
        assert!(error.message.contains("-10..100"), "{}", error.message);
    }
    assert_eq!(
        codec
            .encode(
                &FieldValue::Decimal(d("100")),
                ParameterKind::Numeric,
                PhysicalType::F64
            )
            .unwrap(),
        FieldValue::Float(100.0)
    );
    // A value read beyond the bounds is limited to them.
    assert_eq!(
        codec.decode(&FieldValue::Float(120.0), ParameterKind::Numeric).unwrap(),
        FieldValue::Decimal(d("100"))
    );
}
#[test]
fn half_up_zero_digits_and_signed_rounding() {
    let c = ValueCodec {
        digits: Some(0),
        ..Default::default()
    };
    assert_eq!(
        c.decode(&FieldValue::Decimal(d("-2.5")), ParameterKind::Numeric)
            .unwrap(),
        FieldValue::Decimal(d("-3"))
    );
}
#[test]
fn integer_and_json_precision() {
    let c = ValueCodec::default();
    let v = FieldValue::Unsigned(u64::MAX);
    let n = c.decode(&v, ParameterKind::Numeric).unwrap();
    assert_eq!(c.encode(&n, ParameterKind::Numeric, PhysicalType::U64).unwrap(), v);
    let json = payload::to_json(&v).unwrap();
    assert_eq!(json.to_string(), u64::MAX.to_string());
    assert_eq!(payload::from_json(&json).unwrap(), v);
    let v = FieldValue::Decimal(d("12345678901234567890.12345678"));
    assert_eq!(payload::from_json(&payload::to_json(&v).unwrap()).unwrap(), v);
}
#[test]
fn invalid_values_never_silently_coerce() {
    let c = ValueCodec::default();
    assert!(c
        .encode(&FieldValue::Unsigned(256), ParameterKind::Numeric, PhysicalType::U8)
        .is_err());
    assert!(c
        .encode(&FieldValue::Float(1.5), ParameterKind::Numeric, PhysicalType::I16)
        .is_err());
    assert!(c.decode(&FieldValue::Float(f64::NAN), ParameterKind::Numeric).is_err());
    assert!(ValueCodec {
        scale: Decimal::ZERO,
        ..c
    }
    .validate()
    .is_err());
}
#[test]
fn indicator_reverse_is_symmetric() {
    let c = ValueCodec {
        reverse_logic: true,
        ..Default::default()
    };
    let v = c.decode(&FieldValue::Bool(true), ParameterKind::Indicator).unwrap();
    assert_eq!(v, FieldValue::Bool(false));
    assert_eq!(
        c.encode(&v, ParameterKind::Indicator, PhysicalType::Bool).unwrap(),
        FieldValue::Bool(true)
    );
}
#[test]
fn plc_logs_preserve_identity_enum_and_device_event() {
    let r = common::request(Protocol::Mqtt, "mqtt://localhost".into(), Operation::Subscribe);
    let b=payload::plc_log_batch(&r,br#"{"1":{"loggerName":"test-device","source":"exact-id","type":1,"numericValue":18446744073709551615,"logEventDate":"DT#2026-10-18-12:30:00"},"2":{"loggerName":"test-device","source":"","type":3,"message":"started"},"3":{"loggerName":"test-device","source":"status","type":2,"enumValue":"0010"}}"#).unwrap();
    assert_eq!(b.samples[0].parameter_id.as_deref(), Some("exact-id"));
    assert_eq!(b.samples[0].value, FieldValue::Unsigned(u64::MAX));
    assert!(b.samples[0].source_time.is_some());
    assert!(b.samples[1].parameter_id.is_none());
    assert!(matches!(b.samples[1].value, FieldValue::Record(_)));
    assert_eq!(b.samples[2].value, FieldValue::Text("0010".into()));
    // Decoding preserves the identity; the Moqui binding checks group membership before persistence.
    let child = payload::plc_log_batch(&r, br#"{"1":{"loggerName":"child-device","message":"event"}}"#).unwrap();
    assert_eq!(child.samples[0].device_id, "child-device");
}
#[test]
fn connection_debug_redacts_credentials_and_legacy_uri_secrets() {
    let mut r = common::request(Protocol::Mqtt, "mqtt://name:secret@localhost".into(), Operation::Read);
    r.connection.credentials.password = Some("another-secret".into());
    r.connection.options.insert("password".into(), "legacy-secret".into());
    let debug = format!("{:?}", r);
    assert!(!debug.contains("secret"));
}
#[test]
fn mqtt_inbound_accepts_legacy_scalars_envelopes_and_rows_but_not_wrong_ids() {
    let r = common::request(Protocol::Mqtt, "mqtt://localhost".into(), Operation::Read);
    for bytes in [
        &b"18446744073709551615"[..],
        &br#"{"numericValue":18446744073709551615,"parameterId":"test-param"}"#[..],
        &br#"[18446744073709551615]"#[..],
    ] {
        let out = payload::read_payload(&r, bytes).unwrap();
        assert_eq!(out[0].value, Some(FieldValue::Unsigned(u64::MAX)));
    }
    let out = payload::read_payload(&r, br#"{"parameterId":"other","numericValue":10}"#).unwrap();
    assert_eq!(out[0].status, ItemStatus::Failed);
}
#[test]
fn mqtt_bit_and_indicator_envelopes_match_gateway_fields() {
    let mut r = common::request(Protocol::Mqtt, "mqtt://localhost".into(), Operation::Write);
    r.items[0].kind = ParameterKind::Bits;
    r.items[0].physical_type = PhysicalType::U8;
    let mut input = RequestInput {
        values: std::collections::BTreeMap::from([("test-param".into(), FieldValue::Unsigned(6))]),
        ..Default::default()
    };
    let p: serde_json::Value =
        serde_json::from_slice(&payload::write_item_payload(&r.items[0], &input).unwrap()).unwrap();
    assert_eq!(p["parameterEnumId"], "00000110");
    assert_eq!(p["temperature"], "00000110");
    assert_eq!(
        ValueCodec::default()
            .decode(&FieldValue::Text("00000110".into()), ParameterKind::Bits)
            .unwrap(),
        FieldValue::Unsigned(6)
    );
    r.items[0].kind = ParameterKind::Indicator;
    r.items[0].physical_type = PhysicalType::Bool;
    input.values.insert("test-param".into(), FieldValue::Bool(true));
    let p: serde_json::Value =
        serde_json::from_slice(&payload::write_item_payload(&r.items[0], &input).unwrap()).unwrap();
    assert_eq!(p["symbolicValue"], "Y");
    assert_eq!(p["temperature"], true);
}
