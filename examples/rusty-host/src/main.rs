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

//! Independent consumer: no Moqui runtime, database, Entity Engine or service registry.

use moqui_device_gateway::{codec::ValueCodec, *};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::args()
        .nth(1)
        .ok_or("Supply a Modbus TCP endpoint host:port")?;
    let request = Arc::new(PreparedDeviceRequest {
        name: "maintenance-read".into(),
        device_id: "configured-device-id".into(),
        parent_request: None,
        operation: Operation::Read,
        subscription_type: Default::default(),
        connection: Connection {
            name: "field-connection".into(),
            protocol: Protocol::ModbusTcp,
            endpoint,
            options: BTreeMap::from([("unit-identifier".into(), "1".into())]),
            credentials: Credentials::default(),
        },
        query: None,
        items: vec![Item {
            parameter_id: "configured-parameter-id".into(),
            name: "register".into(),
            address: "holding-register:0:UINT".into(),
            physical_type: PhysicalType::U16,
            kind: ParameterKind::Numeric,
            codec: ValueCodec::default(),
            bulk_index: None,
        }],
        timeout: Duration::from_millis(500),
        polling_interval: Duration::from_millis(100),
        logging: false,
        plc_log: false,
        only_changed: false,
    });
    let facade = Arc::new(DeviceGatewayFacade::new());
    let mut output = RequestOutput::default();
    let result = facade
        .execute(
            request.clone(),
            &RequestInput::default(),
            &mut output,
            request.deadline(),
            None,
        )
        .await;
    facade.close().await;
    println!("{:?}: {:?}", result?, output.items);
    Ok(())
}
