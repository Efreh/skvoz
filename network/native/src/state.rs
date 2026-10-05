//! Anchored root-owned storage; paths are trusted startup configuration only.
use crate::invalid;
use std::ffi::{CString, OsStr};
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path};

#[derive(Debug)]
pub struct SecureStateDir {
    directory: OwnedFd,
    _lock: OwnedFd,
}

impl SecureStateDir {
    /// Every ancestor must be a root-owned directory not writable by group/other.
    /// The leaf is mode0700. The caller provisions it before dropping privileges.
    pub fn open(path: &Path) -> io::Result<Self> {
        let directory = root_directory(path, true)?;
        let lock = open_file(
            directory.as_raw_fd(),
            "lock",
            libc::O_RDWR | libc::O_CREAT,
            0o600,
        )?;
        validate_file(&lock)?;
        // SAFETY: flock accepts an owned descriptor and scalar operation flags.
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            directory,
            _lock: lock,
        })
    }

    /// Open only an existing state/lock without creating either. Missing state
    /// returns None after secure absence or a wholly empty provisioned leaf.
    /// Unknown files, a missing lock in nonempty state, or an insecure lock fail.
    /// The caller disables activation and repeats its removal check to prevent
    /// a fresh initialization race in the never-initialized case.
    pub fn open_existing(path: &Path) -> io::Result<Option<Self>> {
        let directory = match root_directory(path, true) {
            Ok(fd) => fd,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let lock = match open_file(directory.as_raw_fd(), "lock", libc::O_RDONLY, 0) {
            Ok(fd) => fd,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // Package provisioning can create the private directory before
                // the helper first runs. Only an anchored, completely empty
                // directory qualifies; admission must already be disabled by
                // the caller or checked again after disabling activation.
                if directory_entries(&directory)?.is_empty() {
                    return Ok(None);
                }
                return Err(e);
            }
            Err(e) => return Err(e),
        };
        validate_file(&lock)?;
        // SAFETY: flock accepts a live owned FD and scalar nonblocking flags.
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Some(Self {
            directory,
            _lock: lock,
        }))
    }

    pub fn read(&self, name: &str, cap: usize) -> io::Result<Option<Vec<u8>>> {
        let fd = match open_file(self.directory.as_raw_fd(), name, libc::O_RDONLY, 0) {
            Ok(fd) => fd,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        validate_file(&fd)?;
        let mut body = Vec::new();
        File::from(fd)
            .take(cap.saturating_add(1) as u64)
            .read_to_end(&mut body)?;
        if body.len() > cap {
            return Err(invalid("state file exceeds limit"));
        }
        Ok(Some(body))
    }

    /// Atomic replacement with file and parent-directory durability. A stale
    /// temporary file never becomes authoritative; its presence is inspectable.
    pub fn replace(&self, name: &str, body: &[u8]) -> io::Result<()> {
        // An anchored directory is inaccessible to unprivileged writers. Check
        // the existing target before replacement, including hard-link and type
        // restrictions; a root administrator remains a trusted authority.
        match open_file(self.directory.as_raw_fd(), name, libc::O_RDONLY, 0) {
            Ok(existing) => validate_file(&existing)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let name = filename(name)?;
        let mut random = [0u8; 16];
        let mut used = 0;
        while used < random.len() {
            // SAFETY: the tail is a writable allocation for its declared length.
            let count = unsafe {
                libc::getrandom(random[used..].as_mut_ptr().cast(), random.len() - used, 0)
            };
            if count < 0 {
                let e = io::Error::last_os_error();
                if e.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(e);
            }
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "random source EOF",
                ));
            }
            used += count as usize;
        }
        let temporary = format!(
            "pending-{}",
            random
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
        let fd = open_file(
            self.directory.as_raw_fd(),
            &temporary,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            0o600,
        )?;
        validate_file(&fd)?;
        let mut file = File::from(fd);
        let result = (|| {
            file.write_all(body)?;
            file.sync_all()?;
            let temp = filename(&temporary)?;
            // SAFETY: both NUL-terminated names are relative to the live anchor.
            if unsafe {
                libc::renameat(
                    self.directory.as_raw_fd(),
                    temp.as_ptr(),
                    self.directory.as_raw_fd(),
                    name.as_ptr(),
                )
            } < 0
            {
                return Err(io::Error::last_os_error());
            }
            sync_directory(self.directory.as_raw_fd())
        })();
        if result.is_err() {
            let temp = filename(&temporary)?;
            // SAFETY: only this operation's freshly created relative name is removed.
            unsafe {
                libc::unlinkat(self.directory.as_raw_fd(), temp.as_ptr(), 0);
            }
        }
        result
    }

    pub fn entries(&self) -> io::Result<Vec<String>> {
        directory_entries(&self.directory)
    }
}

fn directory_entries(directory: &OwnedFd) -> io::Result<Vec<String>> {
    // dup would share the directory cursor and lose entries on a second
    // enumeration. openat(".") creates a new open-file description.
    let dot = CString::new(".").expect("literal");
    // SAFETY: the live anchor and constant name open a fresh directory FD.
    let raw = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            dot.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    let copied = own(raw)?;
    use std::os::fd::IntoRawFd;
    let raw = copied.into_raw_fd();
    // SAFETY: raw is a fresh owned directory FD. fdopendir owns it on success.
    let dir = unsafe { libc::fdopendir(raw) };
    if dir.is_null() {
        // SAFETY: fdopendir failed and did not take ownership.
        unsafe {
            libc::close(raw);
        }
        return Err(io::Error::last_os_error());
    }
    let mut entries = Vec::new();
    let result = loop {
        // SAFETY: errno is thread-local; resetting it distinguishes EOF/error.
        unsafe {
            *libc::__errno_location() = 0;
        }
        // SAFETY: dir remains a valid DIR until closed below.
        let entry = unsafe { libc::readdir(dir) };
        if entry.is_null() {
            let error = io::Error::last_os_error();
            break if error.raw_os_error() == Some(0) {
                Ok(entries)
            } else {
                Err(error)
            };
        }
        // SAFETY: readdir returns a NUL-terminated name live until next call.
        let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) };
        let name = match name.to_str() {
            Ok(s) => s,
            Err(_) => break Err(invalid("non-UTF8 state entry")),
        };
        if name != "." && name != ".." {
            entries.push(name.to_owned());
        }
        if entries.len() > 8192 {
            break Err(invalid("too many state entries"));
        }
    };
    // SAFETY: exactly one close for this owned DIR and its descriptor.
    unsafe {
        libc::closedir(dir);
    }
    result
}

/// Read a root-owned0600 static config through non-writable no-follow ancestors.
pub fn read_root_config(path: &Path, cap: usize) -> io::Result<Vec<u8>> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("missing config parent"))?;
    let directory = root_directory(parent, false)?;
    let name = path
        .file_name()
        .and_then(OsStr::to_str)
        .ok_or_else(|| invalid("invalid config name"))?;
    let fd = open_file(directory.as_raw_fd(), name, libc::O_RDONLY, 0)?;
    validate_file(&fd)?;
    let mut body = Vec::new();
    File::from(fd)
        .take(cap.saturating_add(1) as u64)
        .read_to_end(&mut body)?;
    if body.len() > cap {
        return Err(invalid("config exceeds limit"));
    }
    Ok(body)
}

/// Read a private runtime config owned by the current effective UID. Every
/// ancestor must belong to root or that UID and exclude group/other writes.
/// Resolving component by component with no-follow prevents symlink substitution.
pub fn read_private_config(path: &Path, cap: usize) -> io::Result<Vec<u8>> {
    // SAFETY: geteuid takes no arguments and cannot alter process state.
    let uid = unsafe { libc::geteuid() };
    let parent = path
        .parent()
        .ok_or_else(|| invalid("missing config parent"))?;
    let directory = owned_directory(parent, true, uid)?;
    let name = path
        .file_name()
        .and_then(OsStr::to_str)
        .ok_or_else(|| invalid("invalid config name"))?;
    let fd = open_file(directory.as_raw_fd(), name, libc::O_RDONLY, 0)?;
    let metadata = stat(&fd)?;
    validate_file_owner(&metadata, uid)?;
    let mut body = Vec::new();
    File::from(fd)
        .take(cap.saturating_add(1) as u64)
        .read_to_end(&mut body)?;
    if body.len() > cap {
        return Err(invalid("config exceeds limit"));
    }
    Ok(body)
}

fn root_directory(path: &Path, private: bool) -> io::Result<OwnedFd> {
    owned_directory(path, private, 0)
}
fn owned_directory(path: &Path, private: bool, uid: u32) -> io::Result<OwnedFd> {
    if !path.is_absolute()
        || path
            .components()
            .any(|p| !matches!(p, Component::RootDir | Component::Normal(_)))
    {
        return Err(invalid("state directory must be absolute"));
    }
    let root = CString::new("/").expect("literal");
    // SAFETY: root is a NUL-terminated literal and flags open a directory only.
    let raw = unsafe {
        libc::open(
            root.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    let mut fd = own(raw)?;
    validate_directory_owner(&fd, false, uid)?;
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                let name =
                    CString::new(name.as_bytes()).map_err(|_| invalid("invalid directory name"))?;
                // SAFETY: name is relative, NUL-terminated, and anchor remains live.
                let raw = unsafe {
                    libc::openat(
                        fd.as_raw_fd(),
                        name.as_ptr(),
                        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                    )
                };
                fd = own(raw)?;
                validate_directory_owner(&fd, false, uid)?;
            }
            _ => return Err(invalid("noncanonical state path")),
        }
    }
    validate_directory_owner(&fd, private, uid)?;
    Ok(fd)
}

fn stat(fd: &OwnedFd) -> io::Result<libc::stat> {
    // SAFETY: stat is a plain C output structure initialized by fstat.
    let mut value: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: value is aligned and writable for the full advertised stat size.
    if unsafe { libc::fstat(fd.as_raw_fd(), &mut value) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(value)
}
fn validate_directory_owner(fd: &OwnedFd, private: bool, uid: u32) -> io::Result<()> {
    let s = stat(fd)?;
    if (s.st_uid != 0 && s.st_uid != uid)
        || s.st_mode & libc::S_IFMT != libc::S_IFDIR
        || (s.st_mode & 0o022 != 0
            && !(uid != 0 && !private && s.st_uid == 0 && s.st_mode & 0o1777 == 0o1777))
        || (private && (s.st_mode & 0o7777 != 0o700 || s.st_uid != uid))
    {
        return Err(invalid("insecure root directory"));
    }
    Ok(())
}
fn validate_file(fd: &OwnedFd) -> io::Result<()> {
    validate_file_owner(&stat(fd)?, 0)
}
fn validate_file_owner(s: &libc::stat, uid: u32) -> io::Result<()> {
    if s.st_uid != uid
        || s.st_mode & libc::S_IFMT != libc::S_IFREG
        || s.st_mode & 0o7777 != 0o600
        || s.st_nlink != 1
    {
        return Err(invalid("insecure root file"));
    }
    Ok(())
}
fn filename(name: &str) -> io::Result<CString> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.len() > 128
        || !name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
    {
        return Err(invalid("invalid state filename"));
    }
    CString::new(name).map_err(|_| invalid("invalid state filename"))
}
fn open_file(directory: i32, name: &str, flags: i32, mode: u32) -> io::Result<OwnedFd> {
    let name = filename(name)?;
    // SAFETY: the validated relative name and live anchor cannot escape via symlinks.
    let raw = unsafe {
        libc::openat(
            directory,
            name.as_ptr(),
            flags | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            mode,
        )
    };
    own(raw)
}
fn own(raw: i32) -> io::Result<OwnedFd> {
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful open/openat returns a fresh uniquely owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}
fn sync_directory(raw: i32) -> io::Result<()> {
    // SAFETY: fsync takes a live descriptor and has no pointer argument.
    if unsafe { libc::fsync(raw) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

// Listener parents allow traversal while excluding unprivileged replacement.
pub(crate) fn validate_root_parent(path: &Path) -> io::Result<()> {
    root_directory(path, false).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_foreign_type_mode_link_and_owner_before_atomic_replacement() {
        // SAFETY: zero is a valid initial value for the plain stat structure.
        let mut s: libc::stat = unsafe { std::mem::zeroed() };
        s.st_uid = 0;
        s.st_mode = libc::S_IFREG | 0o600;
        s.st_nlink = 1;
        assert!(validate_file_owner(&s, 0).is_ok());
        for mode in [
            libc::S_IFDIR | 0o600,
            libc::S_IFREG | 0o644,
            libc::S_IFREG | 0o4600,
        ] {
            s.st_mode = mode;
            assert!(validate_file_owner(&s, 0).is_err());
        }
        s.st_mode = libc::S_IFREG | 0o600;
        s.st_nlink = 2;
        assert!(validate_file_owner(&s, 0).is_err());
        s.st_nlink = 1;
        s.st_uid = 1000;
        assert!(validate_file_owner(&s, 0).is_err());
        for name in ["", ".", "..", "a/b", "a\\b", "x\0y"] {
            assert!(filename(name).is_err());
        }
    }
}
