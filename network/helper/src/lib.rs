//! Scoped helper operations for the shared network runtime.
#![forbid(unsafe_code)]

mod command;
pub mod control_loop;
pub mod firewall;
pub mod journal;
pub mod kernel;
pub mod registry;
pub mod service;

#[derive(Debug)]
pub enum HelperError {
    InvalidRequest,
    Forbidden,
    InvalidState,
    LeaseStoreInvalid,
    Overloaded,
    Io(std::io::Error),
}
impl std::fmt::Display for HelperError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "helper error: {self:?}")
    }
}
impl std::error::Error for HelperError {}
impl From<std::io::Error> for HelperError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
pub type Result<T> = std::result::Result<T, HelperError>;
