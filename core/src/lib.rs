//! Universal SKVOZ byte streams, sans-I/O admission and optional NATS runtimes.
//!
//! Stream/Manager hosts own transport and the clock. Optional NATS runtimes
//! provide routing, bounded transport driving and session lifecycle. This crate
//! owns stream state, byte credit and admission. Frames are an experimental API,
//! not a stable wire format. Polling data does not acknowledge consumption.

mod manager;
#[cfg(feature = "nats")]
pub mod nats;
#[cfg(feature = "nats")]
pub mod runtime;
#[cfg(feature = "nats")]
mod runtime_tls;
mod stream;
mod types;
pub mod wire;

pub use manager::*;
pub use stream::Stream;
pub use types::*;
