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

//! Field communication drivers with a common facade, independent of Moqui.
//!
//! Each protocol is a backend implementing [`DeviceFacade`] (MQTT, OPC UA, Modbus TCP), each behind its Cargo feature.
//! [`DeviceGatewayFacade`] is the registry of their connections and subscriptions and gives them one API:
//! execute a read, write, subscription or unsubscription, with continuous samples delivered to a [`SampleSink`].
//! Any Rust host creates one with a Tokio runtime. Under the `moqui` feature, [`DeviceGatewayToolFactory`] holds one
//! for MR-Moqui; the services, persistence and notifications are those of moqui-device.
pub mod backends;
pub mod client;
pub mod codec;
pub mod device_facade;
pub mod model;
pub mod payload;
#[cfg(feature = "moqui")]
pub mod tool;

pub use client::DeviceGatewayFacade;
pub use device_facade::{DeviceFacade, Receipt, SampleSink};
pub use model::*;
#[cfg(feature = "moqui")]
pub use tool::DeviceGatewayToolFactory;

#[cfg(feature = "moqui")]
mod native {
    use moqui_core::{
        context::Error,
        native::{submit_component, NativeComponent, NativeRegistry},
    };
    submit_component! { NativeComponent { name: "moqui-device-gateway", register } }
    fn register(registry: &mut NativeRegistry) -> Result<(), Error> {
        registry.tool("moqui_device_gateway::DeviceGatewayToolFactory", || {
            std::sync::Arc::new(crate::tool::DeviceGatewayToolFactory::default())
        })
    }
}
