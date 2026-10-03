pub mod app;
pub mod backend;
pub mod desktop;
pub mod enrollment;
pub mod ipc;
pub mod proxy;
pub mod settings;
pub mod telemetry;
pub mod tray;
pub mod ui;
pub const DAEMON_VERSION: &str = "1.4.0";
pub const RECEIVE_WINDOW: usize = 1024 * 1024;
pub const DATA_BLOCK: usize = 32 * 1024;
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
    fn from(_: std::io::Error) -> Self {
        Self("io_failed")
    }
}
