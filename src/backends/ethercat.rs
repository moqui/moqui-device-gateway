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

//! EtherCAT with ethercrab: this host is the MainDevice of the segment on a raw Ethernet interface
//! (`ethercat://eth0`). The first request on an interface starts the segment: it discovers the SubDevices, brings
//! them to OP and exchanges the process data image every `cycle-time-us` microseconds (default 2000) in a thread of
//! its own. A read takes the image of the last cycle; a write changes the output image and completes when two cycles
//! have sent it.
//!
//! An address is `input:SUBDEVICE:BYTE:TYPE` or `output:SUBDEVICE:BYTE:TYPE`: the SubDevice by its position on the
//! segment (0 is the first), the byte offset in its inputs or outputs, the physical type, little-endian as EtherCAT
//! sends it. A BOOL addresses one bit: `input:1:0.3:BOOL`. Only outputs are written.
//!
//! The cycle is fixed by the request that starts the segment; a request with another `cycle-time-us` is refused. When
//! the host closes, the SubDevices are taken to SAFE-OP and INIT: the outputs are no longer driven, and each SubDevice
//! applies its own safe state. A segment that stops on an error (a cable, a SubDevice leaving OP for good) is started
//! again by a request at least five seconds later.
//!
//! The cycle is not hard real-time: it runs in an ordinary thread, and its jitter is that of the operating system.
//! Opening a raw interface needs the `CAP_NET_RAW` capability (or root) on Linux.
use crate::{device_facade::DeviceFacade, model::*};
use async_trait::async_trait;
use ethercrab::{
    std::{ethercat_now, tx_rx_task},
    MainDevice, MainDeviceConfig, PduStorage, Timeouts,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{watch, Mutex};

const MAX_SUBDEVICES: usize = 64;
const MAX_PDI: usize = 4096;
const MAX_FRAMES: usize = 16;
const MAX_PDU_DATA: usize = PduStorage::element_size(1100);
/// How long a segment that stopped stays stopped before a request starts it again: a request polled at a short
/// interval must not open the interface many times a second.
const RESTART_DELAY: Duration = Duration::from_secs(5);
const DEFAULT_CYCLE_US: u64 = 2000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Area {
    Input,
    Output,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Address {
    pub area: Area,
    pub subdevice: usize,
    pub byte: usize,
    pub bit: Option<u8>,
    pub ty: PhysicalType,
}

impl Address {
    pub fn parse(text: &str) -> Result<Self, DeviceGatewayError> {
        let p: Vec<_> = text.split(':').collect();
        if p.len() != 4 {
            return Err(DeviceGatewayError::invalid(
                "EtherCAT address must be input|output:SUBDEVICE:BYTE[.BIT]:TYPE",
            ));
        }
        let area = match p[0] {
            "input" => Area::Input,
            "output" => Area::Output,
            _ => return Err(DeviceGatewayError::invalid("EtherCAT area must be input or output")),
        };
        let subdevice = p[1]
            .parse::<usize>()
            .map_err(|_| DeviceGatewayError::invalid("Invalid EtherCAT SubDevice position"))?;
        let (byte, bit) = match p[2].split_once('.') {
            Some((byte, bit)) => (
                byte,
                Some(
                    bit.parse::<u8>()
                        .ok()
                        .filter(|b| *b < 8)
                        .ok_or_else(|| DeviceGatewayError::invalid("EtherCAT bit must be 0..7"))?,
                ),
            ),
            None => (p[2], None),
        };
        let byte = byte
            .parse::<usize>()
            .map_err(|_| DeviceGatewayError::invalid("Invalid EtherCAT byte offset"))?;
        let ty = PhysicalType::parse(p[3])?;
        if (ty == PhysicalType::Bool) != bit.is_some() {
            return Err(DeviceGatewayError::invalid(
                "A BOOL addresses a bit (BYTE.BIT); other types a byte",
            ));
        }
        width(ty)?;
        Ok(Self {
            area,
            subdevice,
            byte,
            bit,
            ty,
        })
    }
}

fn width(ty: PhysicalType) -> Result<usize, DeviceGatewayError> {
    Ok(match ty {
        PhysicalType::Bool | PhysicalType::U8 | PhysicalType::I8 => 1,
        PhysicalType::U16 | PhysicalType::I16 => 2,
        PhysicalType::U32 | PhysicalType::I32 | PhysicalType::F32 => 4,
        PhysicalType::U64 | PhysicalType::I64 | PhysicalType::F64 => 8,
        PhysicalType::Text | PhysicalType::Bytes => {
            return Err(DeviceGatewayError::unsupported(
                "EtherCAT process data holds numbers and bits",
            ))
        }
    })
}

/// The value at an address of a process data image.
pub fn decode(image: &[u8], a: &Address) -> Result<FieldValue, DeviceGatewayError> {
    let n = width(a.ty)?;
    let bytes = image
        .get(a.byte..a.byte + n)
        .ok_or_else(|| DeviceGatewayError::invalid("EtherCAT address beyond the process data of the SubDevice"))?;
    let mut raw = [0u8; 8];
    raw[..n].copy_from_slice(bytes);
    let u = u64::from_le_bytes(raw);
    Ok(match a.ty {
        PhysicalType::Bool => FieldValue::Bool(bytes[0] >> a.bit.unwrap_or(0) & 1 == 1),
        PhysicalType::U8 | PhysicalType::U16 | PhysicalType::U32 | PhysicalType::U64 => FieldValue::Unsigned(u),
        PhysicalType::I8 => FieldValue::Signed((u as u8 as i8).into()),
        PhysicalType::I16 => FieldValue::Signed((u as u16 as i16).into()),
        PhysicalType::I32 => FieldValue::Signed((u as u32 as i32).into()),
        PhysicalType::I64 => FieldValue::Signed(u as i64),
        PhysicalType::F32 => FieldValue::Float(f32::from_bits(u as u32).into()),
        PhysicalType::F64 => FieldValue::Float(f64::from_bits(u)),
        PhysicalType::Text | PhysicalType::Bytes => unreachable!(),
    })
}

/// Write a physical value at an address of a process data image.
pub fn encode(image: &mut [u8], a: &Address, value: &FieldValue) -> Result<(), DeviceGatewayError> {
    let n = width(a.ty)?;
    let target = image
        .get_mut(a.byte..a.byte + n)
        .ok_or_else(|| DeviceGatewayError::invalid("EtherCAT address beyond the process data of the SubDevice"))?;
    if a.ty == PhysicalType::Bool {
        let FieldValue::Bool(on) = value else {
            return Err(DeviceGatewayError::invalid("A BOOL needs a boolean"));
        };
        let mask = 1u8 << a.bit.unwrap_or(0);
        target[0] = if *on { target[0] | mask } else { target[0] & !mask };
        return Ok(());
    }
    let range = || DeviceGatewayError::invalid("Value outside the type range");
    let integer = |v: &FieldValue| -> Result<i128, DeviceGatewayError> {
        match v {
            FieldValue::Signed(n) => Ok((*n).into()),
            FieldValue::Unsigned(n) => Ok((*n).into()),
            _ => Err(DeviceGatewayError::invalid("An integer needs an integer value")),
        }
    };
    let bits: u64 = match a.ty {
        PhysicalType::F32 | PhysicalType::F64 => {
            let f = match value {
                FieldValue::Float(f) => *f,
                FieldValue::Signed(n) => *n as f64,
                FieldValue::Unsigned(n) => *n as f64,
                _ => return Err(DeviceGatewayError::invalid("A REAL needs a number")),
            };
            if a.ty == PhysicalType::F32 {
                (f as f32).to_bits().into()
            } else {
                f.to_bits()
            }
        }
        PhysicalType::U8 | PhysicalType::U16 | PhysicalType::U32 | PhysicalType::U64 => {
            let v = integer(value)?;
            if v < 0 || (n < 8 && v >= 1i128 << (n * 8)) || v > i128::from(u64::MAX) {
                return Err(range());
            }
            v as u64
        }
        _ => {
            let v = integer(value)?;
            let half = 1i128 << (n * 8 - 1);
            if v < -half || v >= half {
                return Err(range());
            }
            v as i64 as u64
        }
    };
    target.copy_from_slice(&bits.to_le_bytes()[..n]);
    Ok(())
}

/// The process data of the SubDevices, as of the last cycle.
#[derive(Default)]
struct Image {
    /// Set once the SubDevices are in OP.
    ready: bool,
    /// Why the segment stopped, and when; a request after [`RESTART_DELAY`] starts it again.
    failed: Option<(String, Instant)>,
    /// False while a SubDevice is not in OP, so its inputs are not current.
    all_op: bool,
    inputs: Vec<Vec<u8>>,
    outputs: Vec<Vec<u8>>,
}

/// The PDU storage of a segment, owned by its threads and freed once they no longer use it.
struct Storage(*mut PduStorage<MAX_FRAMES, MAX_PDU_DATA>);

// SAFETY: the storage is shared with ethercrab's TX/RX and loop handles, which are themselves sent between threads;
// `Storage` only carries the pointer to free it.
unsafe impl Send for Storage {}

impl Storage {
    /// # Safety
    ///
    /// Nothing may use the storage any more: its loop, TX and RX handles are dropped.
    unsafe fn free(self) {
        drop(unsafe { Box::from_raw(self.0) });
    }
}

struct Segment {
    /// The cycle of the process data exchange, fixed when the segment starts.
    cycle: Duration,
    image: std::sync::Mutex<Image>,
    /// The number of cycles done.
    cycles: watch::Sender<u64>,
    stop: AtomicBool,
}

impl Segment {
    fn new(cycle: Duration) -> Arc<Self> {
        Arc::new(Self {
            cycle,
            image: std::sync::Mutex::new(Image::default()),
            cycles: watch::Sender::new(0),
            stop: AtomicBool::new(false),
        })
    }

    /// A segment that could not start: requests fail with its error until [`RESTART_DELAY`] has passed.
    fn failed(cycle: Duration, error: &DeviceGatewayError) -> Arc<Self> {
        let segment = Self::new(cycle);
        segment.image.lock().unwrap_or_else(|p| p.into_inner()).failed = Some((error.message.clone(), Instant::now()));
        segment
    }

    /// Open the interface and start the TX/RX and cycle threads.
    fn start(interface: String, cycle: Duration) -> Result<Arc<Self>, DeviceGatewayError> {
        let segment = Self::new(cycle);
        // Each start has its own PDU storage, as ethercrab splits a storage only once. It is freed when nothing uses
        // it any more: at once when the interface does not open, or after the cycle and TX/RX threads have ended.
        let storage = Storage(Box::into_raw(Box::new(PduStorage::new())));
        // SAFETY: the storage lives until `Storage::free`, which runs only once the loop, TX and RX are dropped.
        let pdu: &'static PduStorage<MAX_FRAMES, MAX_PDU_DATA> = unsafe { &*storage.0 };
        let (tx, rx, pdu_loop) = pdu
            .try_split()
            .map_err(|()| DeviceGatewayError::transport("EtherCAT PDU storage is in use"))?;
        let task = match tx_rx_task(&interface, tx, rx) {
            Ok(task) => task,
            Err(e) => {
                // End the loop handle's use of the storage before freeing it.
                {
                    let _released = pdu_loop;
                }
                // SAFETY: TX and RX were dropped by the failed call, and the loop above.
                unsafe { storage.free() };
                return Err(DeviceGatewayError::transport(format!(
                    "Cannot open EtherCAT interface {interface} (a raw socket needs CAP_NET_RAW): {e}"
                )));
            }
        };
        let io = match std::thread::Builder::new()
            .name(format!("ethercat-io-{interface}"))
            .spawn(move || futures_lite::future::block_on(task))
        {
            Ok(io) => io,
            Err(e) => {
                // End the loop handle's use of the storage before freeing it.
                {
                    let _released = pdu_loop;
                }
                // SAFETY: the thread did not start, so its task, with TX and RX, was dropped.
                unsafe { storage.free() };
                return Err(DeviceGatewayError::transport(format!(
                    "Cannot start EtherCAT TX/RX: {e}"
                )));
            }
        };
        let worker = segment.clone();
        std::thread::Builder::new()
            .name(format!("ethercat-cycle-{interface}"))
            .stack_size(16 * 1024 * 1024)
            .spawn(move || {
                let maindevice = MainDevice::new(pdu_loop, Timeouts::default(), MainDeviceConfig::default());
                let result = futures_lite::future::block_on(worker.run(&maindevice, cycle));
                if let Err(e) = &result {
                    tracing::error!(interface=%interface, error=%e, "EtherCAT segment stopped");
                }
                worker.image.lock().unwrap_or_else(|p| p.into_inner()).failed = Some((
                    result.err().unwrap_or_else(|| "EtherCAT segment closed".into()),
                    Instant::now(),
                ));
                // SAFETY: the group of this MainDevice ended with `run`, and the released loop is dropped, never used
                // again. Releasing it sets the exit flag, which ends the TX/RX thread.
                let _ = unsafe { maindevice.release_all() };
                match io.join() {
                    Ok(result) => {
                        if let Err(e) = result {
                            tracing::error!(interface=%interface, error=?e, "EtherCAT TX/RX stopped with an error");
                        }
                        // SAFETY: the TX/RX thread has ended and its TX and RX are dropped with `result`.
                        unsafe { storage.free() };
                    }
                    // A panicked TX/RX thread may have left its TX or RX alive: keep the storage.
                    Err(_) => tracing::error!(interface=%interface, "EtherCAT TX/RX thread panicked"),
                }
            })
            .map_err(|e| {
                // The TX/RX thread runs on, so the storage stays allocated; the process lacks threads anyway.
                DeviceGatewayError::transport(format!("Cannot start the EtherCAT cycle: {e}"))
            })?;
        Ok(segment)
    }

    async fn run(&self, maindevice: &MainDevice<'_>, cycle: Duration) -> Result<(), String> {
        let group = maindevice
            .init_single_group::<MAX_SUBDEVICES, MAX_PDI>(ethercat_now)
            .await
            .map_err(|e| format!("EtherCAT discovery failed: {e:?}"))?;
        tracing::info!(subdevices = group.len(), "EtherCAT SubDevices discovered");
        let group = group
            .into_op(maindevice)
            .await
            .map_err(|e| format!("EtherCAT SubDevices did not reach OP: {e:?}"))?;
        {
            let mut image = self.image.lock().unwrap_or_else(|p| p.into_inner());
            for subdevice in group.iter(maindevice) {
                let io = subdevice.io_raw();
                image.inputs.push(io.inputs().to_vec());
                image.outputs.push(io.outputs().to_vec());
            }
            image.ready = true;
        }
        let mut next = Instant::now();
        while !self.stop.load(Ordering::Acquire) {
            {
                let image = self.image.lock().unwrap_or_else(|p| p.into_inner());
                for (index, subdevice) in group.iter(maindevice).enumerate() {
                    let mut io = subdevice.io_raw_mut();
                    if let Some(outputs) = image.outputs.get(index) {
                        io.outputs().copy_from_slice(outputs);
                    }
                }
            }
            let response = group
                .tx_rx(maindevice)
                .await
                .map_err(|e| format!("EtherCAT cycle failed: {e:?}"))?;
            {
                let mut image = self.image.lock().unwrap_or_else(|p| p.into_inner());
                image.all_op = response.all_op();
                for (index, subdevice) in group.iter(maindevice).enumerate() {
                    if let Some(inputs) = image.inputs.get_mut(index) {
                        inputs.copy_from_slice(subdevice.io_raw().inputs());
                    }
                }
            }
            self.cycles.send_modify(|n| *n += 1);
            next += cycle;
            let now = Instant::now();
            if next > now {
                std::thread::sleep(next - now);
            } else {
                next = now;
            }
        }
        let group = group
            .into_safe_op(maindevice)
            .await
            .map_err(|e| format!("EtherCAT SubDevices did not leave OP: {e:?}"))?;
        if let Ok(group) = group.into_pre_op(maindevice).await {
            let _ = group.into_init(maindevice).await;
        }
        Ok(())
    }

    /// Wait for the SubDevices to reach OP, or for the segment to fail.
    async fn ready(&self, deadline: Instant) -> Result<(), DeviceGatewayError> {
        let mut cycles = self.cycles.subscribe();
        loop {
            {
                let image = self.image.lock().unwrap_or_else(|p| p.into_inner());
                if let Some((failed, _)) = &image.failed {
                    return Err(DeviceGatewayError::transport(failed.clone()));
                }
                if image.ready && *cycles.borrow_and_update() > 0 {
                    return Ok(());
                }
            }
            // The segment publishes no cycle before OP: look again shortly.
            let wait = tokio::time::timeout(Duration::from_millis(50), cycles.changed());
            if tokio::time::timeout_at(deadline.into(), wait).await.is_err() {
                return Err(DeviceGatewayError::timeout(false));
            }
        }
    }

    /// Wait until the cycles have sent what was written: the cycle under way may have copied the outputs before.
    async fn sent(&self, deadline: Instant) -> Result<(), DeviceGatewayError> {
        let mut cycles = self.cycles.subscribe();
        let target = *cycles.borrow_and_update() + 2;
        while *cycles.borrow_and_update() < target {
            if let Some((failed, _)) = &self.image.lock().unwrap_or_else(|p| p.into_inner()).failed {
                return Err(DeviceGatewayError::transport(failed.clone()).uncertain());
            }
            tokio::time::timeout_at(deadline.into(), cycles.changed())
                .await
                .map_err(|_| DeviceGatewayError::timeout(true))?
                .map_err(|_| DeviceGatewayError::transport("EtherCAT segment closed").uncertain())?;
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct EtherCatDeviceClient {
    segments: Mutex<HashMap<String, Arc<Segment>>>,
}

/// The interface of an endpoint `ethercat://eth0`.
pub fn interface(endpoint: &str) -> Result<String, DeviceGatewayError> {
    let name = endpoint
        .strip_prefix("ethercat://")
        .unwrap_or(endpoint)
        .trim_end_matches('/');
    if name.is_empty() || name.contains('/') || name.contains(':') {
        return Err(DeviceGatewayError::invalid(
            "EtherCAT endpoint must be ethercat://INTERFACE",
        ));
    }
    Ok(name.to_string())
}

#[async_trait]
impl DeviceFacade for EtherCatDeviceClient {
    fn capabilities(&self) -> Capabilities {
        // The process data is exchanged every cycle: poll a DrtRead to sample it.
        Capabilities {
            read: true,
            write: true,
            subscribe: false,
        }
    }
    async fn execute(
        &self,
        r: &PreparedDeviceRequest,
        input: &RequestInput,
        out: &mut RequestOutput,
        deadline: Instant,
    ) -> Result<(), DeviceGatewayError> {
        let writing = r.operation == Operation::Write;
        for k in r.connection.options.keys() {
            if k != "cycle-time-us" {
                return Err(DeviceGatewayError::unsupported(format!("Unknown EtherCAT option: {k}")));
            }
        }
        let cycle = match r.connection.options.get("cycle-time-us") {
            None => DEFAULT_CYCLE_US,
            Some(text) => text
                .parse::<u64>()
                .ok()
                .filter(|us| (250..=1_000_000).contains(us))
                .ok_or_else(|| DeviceGatewayError::invalid("cycle-time-us must be 250..1000000"))?,
        };
        let cycle = Duration::from_micros(cycle);
        let mut plans = Vec::new();
        for item in &r.items {
            let a = Address::parse(&item.address)?;
            if a.ty != item.physical_type {
                return Err(DeviceGatewayError::invalid("Address and item types disagree"));
            }
            if writing && a.area == Area::Input {
                return Err(DeviceGatewayError::invalid("EtherCAT inputs cannot be written"));
            }
            let value = if writing {
                Some(
                    input
                        .values
                        .get(&item.parameter_id)
                        .ok_or_else(|| DeviceGatewayError::invalid("Missing write value"))?,
                )
            } else {
                None
            };
            plans.push((item, a, value));
        }
        if plans.is_empty() {
            return Err(DeviceGatewayError::invalid("EtherCAT request has no items"));
        }
        let name = interface(&r.connection.endpoint)?;
        let segment = {
            let mut segments = self.segments.lock().await;
            let running = match segments.get(&name) {
                Some(segment) => {
                    let failed = segment.image.lock().unwrap_or_else(|p| p.into_inner()).failed.clone();
                    match failed {
                        Some((failed, at)) if at.elapsed() < RESTART_DELAY => {
                            return Err(DeviceGatewayError::transport(failed));
                        }
                        Some(_) => None,
                        None if segment.cycle != cycle => {
                            return Err(DeviceGatewayError::invalid(format!(
                                "EtherCAT interface {name} already runs at a cycle of {} µs",
                                segment.cycle.as_micros()
                            )));
                        }
                        None => Some(segment.clone()),
                    }
                }
                None => None,
            };
            match running {
                Some(segment) => segment,
                None => match Segment::start(name.clone(), cycle) {
                    Ok(segment) => {
                        segments.insert(name, segment.clone());
                        segment
                    }
                    Err(error) => {
                        // Remember the failure, so that the next requests wait before opening the interface again.
                        segments.insert(name, Segment::failed(cycle, &error));
                        return Err(error);
                    }
                },
            }
        };
        segment.ready(deadline).await?;
        {
            let mut image = segment.image.lock().unwrap_or_else(|p| p.into_inner());
            if !image.all_op {
                return Err(DeviceGatewayError::transport("An EtherCAT SubDevice is not in OP"));
            }
            // Check every address first: a write changes nothing unless all of it applies.
            for (_, a, _) in &plans {
                let area = if a.area == Area::Input {
                    &image.inputs
                } else {
                    &image.outputs
                };
                area.get(a.subdevice)
                    .ok_or_else(|| DeviceGatewayError::invalid("No EtherCAT SubDevice at this position"))?;
            }
            if writing {
                let mut outputs = image.outputs.clone();
                for (_, a, value) in &plans {
                    encode(&mut outputs[a.subdevice], a, value.unwrap())?;
                }
                image.outputs = outputs;
            } else {
                for (item, a, _) in &plans {
                    let area = if a.area == Area::Input {
                        &image.inputs
                    } else {
                        &image.outputs
                    };
                    match decode(&area[a.subdevice], a) {
                        Ok(v) => out.items.push(ItemResult::value(&item.parameter_id, v)),
                        Err(e) => {
                            out.items.push(ItemResult::error(&item.parameter_id, e.clone()));
                            return Err(e);
                        }
                    }
                }
                return Ok(());
            }
        }
        segment.sent(deadline).await?;
        for (item, _, value) in plans {
            out.items
                .push(ItemResult::value(&item.parameter_id, value.unwrap().clone()));
        }
        Ok(())
    }
    async fn close(&self) {
        for segment in std::mem::take(&mut *self.segments.lock().await).into_values() {
            segment.stop.store(true, Ordering::Release);
        }
    }
}
