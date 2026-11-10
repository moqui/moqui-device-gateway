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
//! The ToolFactory of the `moqui` feature: one [`DeviceGatewayFacade`] and the Tokio runtime its backends run on, for
//! the services of moqui-device. It holds no service, entity or persistence logic. A host without Moqui creates the
//! facade and a runtime itself.
use crate::client::DeviceGatewayFacade;
use moqui_core::{
    context::{Error, ExecutionContextFactory},
    native::{Tool, ToolFactory},
};
use moqui_lib::object::Value;
use std::{
    future::Future,
    sync::{Arc, Mutex},
};

/// The facade and its I/O runtime.
pub struct Gateway {
    runtime: tokio::runtime::Runtime,
    facade: Arc<DeviceGatewayFacade>,
}
impl Gateway {
    pub fn facade(&self) -> &Arc<DeviceGatewayFacade> {
        &self.facade
    }
    /// Runs a facade call from a thread outside the runtime, such as a service worker.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }
    pub fn handle(&self) -> tokio::runtime::Handle {
        self.runtime.handle().clone()
    }
}

#[derive(Default)]
pub struct DeviceGatewayToolFactory {
    gateway: Mutex<Option<Arc<Gateway>>>,
}
impl DeviceGatewayToolFactory {
    /// The facade and runtime of this factory, once initialized.
    pub fn gateway(&self) -> Result<Arc<Gateway>, Error> {
        self.gateway
            .lock()
            .map_err(|_| Error::new("Device gateway factory lock poisoned"))?
            .clone()
            .ok_or_else(|| Error::new("Device gateway factory is not initialized"))
    }
    pub fn facade(&self) -> Result<Arc<DeviceGatewayFacade>, Error> {
        Ok(self.gateway()?.facade.clone())
    }
}
struct DeviceGatewayTool;
impl Tool for DeviceGatewayTool {}
impl ToolFactory for DeviceGatewayToolFactory {
    fn name(&self) -> &str {
        "DeviceGateway"
    }
    fn init(&self, _: &Arc<ExecutionContextFactory>) -> Result<(), Error> {
        let mut gateway = self
            .gateway
            .lock()
            .map_err(|_| Error::new("Device gateway factory lock poisoned"))?;
        if gateway.is_some() {
            return Ok(());
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("device-gateway-io")
            .build()
            .map_err(|e| Error::new(e.to_string()))?;
        *gateway = Some(Arc::new(Gateway {
            runtime,
            facade: Arc::new(DeviceGatewayFacade::new()),
        }));
        Ok(())
    }
    fn get_instance(&self, _: &[Value]) -> Result<Arc<dyn Tool>, Error> {
        self.gateway()?;
        Ok(Arc::new(DeviceGatewayTool))
    }
    /// Closes the connections and ends the subscriptions; a durable broker session keeps collecting until the next
    /// start.
    fn destroy(&self) {
        let gateway = self.gateway.lock().ok().and_then(|mut g| g.take());
        if let Some(gateway) = gateway {
            gateway.runtime.block_on(gateway.facade.close());
            if let Ok(gateway) = Arc::try_unwrap(gateway) {
                gateway.runtime.shutdown_background();
            }
        }
    }
}
