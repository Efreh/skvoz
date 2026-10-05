#[cfg(feature = "desktop")]
pub mod app;
pub mod backend;
#[cfg(feature = "desktop")]
pub mod desktop;
pub mod enrollment;
pub mod ipc;

pub mod settings;
pub mod telemetry;
#[cfg(feature = "desktop")]
pub mod tray;
#[cfg(feature = "desktop")]
pub mod ui;
pub const RUNTIME_VERSION: &str = "0.1.0";

pub type Result<T> = std::result::Result<T, Error>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Error(pub &'static str);
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self(match error.kind() {
            std::io::ErrorKind::ConnectionReset => "io_connection_reset",
            std::io::ErrorKind::ConnectionAborted => "io_connection_aborted",
            std::io::ErrorKind::BrokenPipe => "io_broken_pipe",
            std::io::ErrorKind::UnexpectedEof => "io_unexpected_eof",
            std::io::ErrorKind::TimedOut => "io_timeout",
            _ => "io_failed",
        })
    }
}
