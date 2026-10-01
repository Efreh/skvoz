//! A single-stream, sans-I/O building block for SKVOZ Core.
//!
//! The driver owns routing, transport, scheduling, and the clock. This crate
//! owns stream state and byte credit. Frames are an experimental internal API,
//! not a stable wire format. Polling data does not acknowledge consumption.

mod stream;
mod types;
pub mod wire;

pub use stream::Stream;
pub use types::*;
