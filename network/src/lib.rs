//! Shared network records and a single-owner Core/NATS network engine.
//! Packet ownership remains with the embedding host until a complete OS write.
pub mod budget;
mod codec;
pub mod config;
mod engine;
pub mod enrollment;
pub mod local_api;
mod packet;
pub mod policy;
pub use codec::*;
pub use engine::*;
pub use packet::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkError {
    InvalidMetadata,
    UnsupportedVersion,
    UnsupportedType,
    UnsupportedFamily,
    InvalidRecord,
    InvalidPacket,
    InvalidConfiguration,
    InvalidState,
    Forbidden,
    Overloaded,
    Timeout,
    Runtime(skvoz_core::runtime::RuntimeError),
}
impl std::fmt::Display for NetworkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "network error: {self:?}")
    }
}
impl std::error::Error for NetworkError {}
impl From<skvoz_core::runtime::RuntimeError> for NetworkError {
    fn from(value: skvoz_core::runtime::RuntimeError) -> Self {
        Self::Runtime(value)
    }
}

#[cfg(all(
    any(target_os = "linux", target_os = "android"),
    feature = "portable-runtime"
))]
mod proxy;
#[cfg(all(
    any(target_os = "linux", target_os = "android"),
    feature = "portable-runtime"
))]
pub mod runtime;
#[cfg(all(
    any(target_os = "linux", target_os = "android"),
    feature = "portable-runtime"
))]
mod tcp;
#[cfg(all(
    any(target_os = "linux", target_os = "android"),
    feature = "portable-runtime"
))]
pub use runtime::{OwnedMessage, PollError, RuntimeDiagnostics, RuntimeFailure, RuntimeHandle};
