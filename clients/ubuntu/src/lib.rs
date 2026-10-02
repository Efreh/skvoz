pub mod backend;
pub mod enrollment;
pub mod ipc;
pub mod proxy;
pub mod settings;
pub mod ui;
pub const DAEMON_VERSION: &str = "1.3.0";
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
