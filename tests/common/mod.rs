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

#![allow(dead_code)]
use moqui_device_gateway::{codec::ValueCodec, model::*};
use std::{collections::BTreeMap, time::Duration};

pub fn request(protocol: Protocol, endpoint: String, operation: Operation) -> PreparedDeviceRequest {
    PreparedDeviceRequest {
        name: "test-request".into(),
        device_id: "test-device".into(),
        parent_request: None,
        operation,
        subscription_type: Default::default(),
        connection: Connection {
            name: "test-connection".into(),
            protocol,
            endpoint,
            options: BTreeMap::new(),
            credentials: Credentials::default(),
        },
        query: None,
        items: vec![Item {
            parameter_id: "test-param".into(),
            name: "temperature".into(),
            address: "holding-register:2:ULINT".into(),
            physical_type: PhysicalType::U64,
            kind: ParameterKind::Numeric,
            codec: ValueCodec::default(),
            bulk_index: None,
        }],
        timeout: Duration::from_millis(500),
        polling_interval: Duration::from_millis(20),
        logging: false,
        plc_log: false,
        only_changed: false,
    }
}
