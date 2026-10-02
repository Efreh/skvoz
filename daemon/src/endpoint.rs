//! Private Linux pathname endpoints and bounded secret-file input.
use std::{
    fs::{self, File},
    io::{self, Read},
    os::unix::{
        fs::{MetadataExt, PermissionsExt},
        net::UnixListener,
    },
    path::{Component, Path, PathBuf},
};
use tokio::net::UnixStream;

pub fn effective_uid() -> io::Result<u32> {
    let (a, _b) = UnixStream::pair()?;
    Ok(a.peer_cred()?.uid())
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, "unsafe local path")
}
fn no_symlinks(path: &Path, uid: u32) -> io::Result<()> {
    if !path.is_absolute() {
        return Err(invalid());
    }
    let mut p = PathBuf::new();
    for part in path.components() {
        match part {
            Component::RootDir | Component::Normal(_) => p.push(part.as_os_str()),
            _ => return Err(invalid()),
        }
        let m = fs::symlink_metadata(&p)?;
        if m.file_type().is_symlink()
            || (m.is_dir()
                && ((m.uid() != 0 && m.uid() != uid)
                    || (m.mode() & 0o022 != 0 && m.mode() & 0o1000 == 0)))
        {
            return Err(invalid());
        }
    }
    Ok(())
}
pub fn private_parent(path: &Path, uid: u32) -> io::Result<()> {
    let parent = path.parent().ok_or_else(invalid)?;
    no_symlinks(parent, uid)?;
    let m = fs::metadata(parent)?;
    if !m.is_dir() || m.uid() != uid || m.mode() & 0o777 != 0o700 {
        return Err(invalid());
    }
    Ok(())
}
pub fn secret_file(path: &Path, uid: u32, maximum: usize) -> io::Result<Vec<u8>> {
    private_parent(path, uid)?;
    no_symlinks(path, uid)?;
    let before = fs::symlink_metadata(path)?;
    if !before.is_file()
        || before.uid() != uid
        || before.mode() & 0o177 != 0
        || before.mode() & 0o400 == 0
        || before.len() > maximum as u64
    {
        return Err(invalid());
    }
    let file = File::open(path)?;
    let m = file.metadata()?;
    if !m.is_file()
        || m.uid() != uid
        || m.mode() & 0o177 != 0
        || m.mode() & 0o400 == 0
        || before.dev() != m.dev()
        || before.ino() != m.ino()
        || m.len() > maximum as u64
    {
        return Err(invalid());
    }
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err(invalid());
    }
    Ok(bytes)
}

pub fn validate_endpoint_path(path: &Path, uid: u32) -> io::Result<()> {
    if !path.is_absolute()
        || path.as_os_str().as_encoded_bytes().len() > 100
        || path.file_name().is_none()
        || path
            .components()
            .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
    {
        return Err(invalid());
    }
    private_parent(path, uid)
}

pub struct Endpoint {
    pub listener: UnixListener,
    path: PathBuf,
    dev: u64,
    ino: u64,
}
impl Endpoint {
    pub fn bind(path: &Path, uid: u32) -> io::Result<Self> {
        validate_endpoint_path(path, uid)?;
        match fs::symlink_metadata(path) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => (),
            _ => return Err(invalid()),
        }
        let listener = UnixListener::bind(path)?;
        let m = fs::symlink_metadata(path)?;
        let endpoint = Self {
            listener,
            path: path.into(),
            dev: m.dev(),
            ino: m.ino(),
        };
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        endpoint.listener.set_nonblocking(true)?;
        Ok(endpoint)
    }
}
impl Drop for Endpoint {
    fn drop(&mut self) {
        if let Ok(m) = fs::symlink_metadata(&self.path)
            && m.dev() == self.dev
            && m.ino() == self.ino
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}
