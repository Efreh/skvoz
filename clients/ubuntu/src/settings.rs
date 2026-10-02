use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    net::IpAddr,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Preferences {
    pub v: u8,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub http_port: u16,
    pub socks_port: u16,
    pub ca_file: String,
    pub devices: BTreeMap<String, String>,
    #[serde(default)]
    pub passwords: BTreeMap<String, String>,
}
impl Default for Preferences {
    fn default() -> Self {
        Self {
            v: 1,
            host: String::new(),
            port: 4222,
            username: String::new(),
            http_port: 8080,
            socks_port: 1080,
            ca_file: String::new(),
            devices: BTreeMap::new(),
            passwords: BTreeMap::new(),
        }
    }
}

impl Preferences {
    pub fn saved_password(&self, host: &str, port: u16, user: &str) -> String {
        let Ok(host) = host_name(host) else {
            return String::new();
        };
        let Ok(key) = serde_json::to_string(&(host, port, user)) else {
            return String::new();
        };
        self.passwords.get(&key).cloned().unwrap_or_default()
    }
    pub fn remember_password(
        &mut self,
        host: &str,
        port: u16,
        user: &str,
        password: String,
    ) -> Result<()> {
        let key = serde_json::to_string(&(host_name(host)?, port, user))
            .map_err(|_| Error("unsafe_settings"))?;
        if !login(user) || port == 0 || !(12..=72).contains(&password.len()) {
            return Err(Error("invalid_password"));
        }
        if !self.passwords.contains_key(&key) && self.passwords.len() >= 64 {
            return Err(Error("device_limit"));
        }
        self.passwords.insert(key, password);
        Ok(())
    }
    fn valid_passwords(&self) -> bool {
        self.passwords.len() <= 64
            && self.passwords.iter().all(|(key, password)| {
                if !(12..=72).contains(&password.len()) {
                    return false;
                }
                let Ok((host, port, user)) = serde_json::from_str::<(String, u16, String)>(key)
                else {
                    return false;
                };
                port != 0
                    && login(&user)
                    && host_name(&host).is_ok_and(|normalized| normalized == host)
            })
    }
}

pub fn uid() -> Result<u32> {
    let text = fs::read_to_string("/proc/self/status")?;
    text.lines()
        .find_map(|line| {
            line.strip_prefix("Uid:")
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|value| value.parse().ok())
        })
        .ok_or(Error("unsafe_settings"))
}
pub fn private_dir(path: &Path) -> Result<()> {
    if !path.is_absolute()
        || path.components().any(|part| {
            matches!(
                part,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
    {
        return Err(Error("unsafe_settings"));
    }
    if !path.exists() {
        fs::DirBuilder::new()
            .mode(0o700)
            .recursive(true)
            .create(path)?;
    }
    for parent in path.ancestors() {
        let meta = fs::symlink_metadata(parent)?;
        if !meta.is_dir()
            || meta.file_type().is_symlink()
            || ![0, uid()?].contains(&meta.uid())
            || meta.mode() & 0o022 != 0 && meta.mode() & 0o1000 == 0
        {
            return Err(Error("unsafe_settings"));
        }
    }
    let meta = fs::metadata(path)?;
    if meta.uid() != uid()? || meta.mode() & 0o777 != 0o700 {
        return Err(Error("unsafe_settings"));
    }
    Ok(())
}
use std::os::unix::fs::DirBuilderExt;
pub fn read_private(path: &Path, cap: usize) -> Result<Vec<u8>> {
    private_dir(path.parent().ok_or(Error("unsafe_settings"))?)?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.uid() != uid()?
        || meta.mode() & 0o777 != 0o600
        || meta.len() > cap as u64
    {
        return Err(Error("unsafe_settings"));
    }
    let mut bytes = Vec::new();
    file.take(cap as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > cap {
        return Err(Error("unsafe_settings"));
    }
    Ok(bytes)
}
pub fn token() -> Result<String> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).map_err(|_| Error("random_failed"))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}
pub fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or(Error("unsafe_settings"))?;
    private_dir(parent)?;
    let temporary = parent.join(format!(".write-{}", token()?));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    let _ = fs::remove_file(temporary);
    result
}
pub fn host_name(input: &str) -> Result<String> {
    let input = input.trim();
    let input = input
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(input);
    if input.is_empty() || input.len() > 253 || input.contains('%') {
        return Err(Error("invalid_address"));
    }
    if let Ok(ip) = input.parse::<IpAddr>() {
        return Ok(ip.to_string());
    }
    if input.contains(':')
        || input
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'.')
    {
        return Err(Error("invalid_address"));
    }
    let input = input.trim_end_matches('.').to_ascii_lowercase();
    if !input.is_ascii()
        || !input.split('.').all(|part| {
            !part.is_empty()
                && part.len() <= 63
                && part.as_bytes()[0].is_ascii_alphanumeric()
                && part.as_bytes()[part.len() - 1].is_ascii_alphanumeric()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
    {
        return Err(Error("invalid_address"));
    }
    Ok(input)
}
pub fn port(input: &str, local: bool) -> Result<u16> {
    if input.is_empty() || !input.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error("invalid_port"));
    }
    let value = input.parse::<u16>().map_err(|_| Error("invalid_port"))?;
    if value < if local { 1024 } else { 1 } {
        return Err(Error("invalid_port"));
    }
    Ok(value)
}
pub fn ports(http: u16, socks: u16) -> Result<()> {
    if http < 1024 || socks < 1024 {
        return Err(Error("invalid_port"));
    }
    if http == socks {
        return Err(Error("port_conflict"));
    }
    Ok(())
}
pub fn login(input: &str) -> bool {
    !input.is_empty()
        && input.len() <= 64
        && input != "__skvoz_server"
        && input
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

pub struct Settings {
    pub directory: PathBuf,
    pub value: Preferences,
    _lock: File,
}
impl Settings {
    pub fn open(directory: PathBuf) -> Result<Self> {
        private_dir(&directory)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(directory.join("instance.lock"))?;
        let meta = lock.metadata()?;
        if !meta.is_file() || meta.uid() != uid()? || meta.mode() & 0o777 != 0o600 {
            return Err(Error("unsafe_settings"));
        }
        lock.try_lock().map_err(|_| Error("already_running"))?;
        let path = directory.join("settings.json");
        let value: Preferences = if path.exists() || path.is_symlink() {
            serde_json::from_slice(&read_private(&path, 32768)?)
                .map_err(|_| Error("unsafe_settings"))?
        } else {
            Preferences::default()
        };
        if value.v != 1
            || value.port == 0
            || !value.valid_passwords()
            || value.devices.len() > 64
            || value.devices.values().any(|token| !valid_token(token))
            || value.host.len() > 253
            || value.username.len() > 64
            || value.ca_file.len() > 4096
        {
            return Err(Error("unsafe_settings"));
        }
        ports(value.http_port, value.socks_port)?;
        Ok(Self {
            directory,
            value,
            _lock: lock,
        })
    }
    pub fn default_path() -> Result<PathBuf> {
        let base = std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|path| PathBuf::from(path).join(".config")))
            .ok_or(Error("unsafe_settings"))?;
        Ok(base.join("skvoz"))
    }
    pub fn save(&mut self, candidate: Preferences) -> Result<()> {
        ports(candidate.http_port, candidate.socks_port)?;
        if !candidate.valid_passwords() {
            return Err(Error("unsafe_settings"));
        }
        let bytes = serde_json::to_vec(&candidate).map_err(|_| Error("unsafe_settings"))?;
        if bytes.len() > 32768 {
            return Err(Error("unsafe_settings"));
        }
        write_private(&self.directory.join("settings.json"), &bytes)?;
        self.value = candidate;
        Ok(())
    }
    pub fn device(&mut self, host: &str, port: u16, user: &str) -> Result<String> {
        let key =
            serde_json::to_string(&(host, port, user)).map_err(|_| Error("unsafe_settings"))?;
        if let Some(value) = self.value.devices.get(&key) {
            return Ok(value.clone());
        }
        if self.value.devices.len() >= 64 {
            return Err(Error("device_limit"));
        }
        let token = token()?;
        let mut candidate = self.value.clone();
        candidate.devices.insert(key, token.clone());
        self.save(candidate)?;
        Ok(token)
    }
}
impl Drop for Settings {
    fn drop(&mut self) {
        // Explicit unlock also releases a lock inherited by a concurrent fork
        // before its close-on-exec descriptors have been closed.
        let _ = self._lock.unlock();
    }
}

pub fn valid_token(input: &str) -> bool {
    input.len() == 32
        && input
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn addresses_and_ports() {
        assert_eq!(host_name("[::1]"), Ok("::1".into()));
        for value in ["127.1", "bad host", "fe80::1%lo", "tls://host"] {
            assert!(host_name(value).is_err());
        }
        for value in ["1.5", "+1", " 1", "0", "65536"] {
            assert!(port(value, false).is_err());
        }
        assert!(ports(8080, 8080).is_err());
        assert!(!login("__skvoz_server"));
    }
}

#[cfg(test)]
mod persistence_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    #[test]
    fn legacy_settings_and_private_secret_validation() {
        let directory = std::env::temp_dir().join(format!("skvoz-settings-{}", token().unwrap()));
        let settings = Settings::open(directory.clone()).unwrap();
        let mut legacy = serde_json::to_value(&settings.value).unwrap();
        legacy.as_object_mut().unwrap().remove("passwords");
        write_private(
            &directory.join("settings.json"),
            &serde_json::to_vec(&legacy).unwrap(),
        )
        .unwrap();
        drop(settings);
        let mut settings = Settings::open(directory.clone()).unwrap();
        assert!(settings.value.passwords.is_empty());
        let mut candidate = settings.value.clone();
        candidate
            .passwords
            .insert("invalid-key".into(), "process-test-password".into());
        assert!(settings.save(candidate).is_err());
        drop(settings);
        fs::set_permissions(
            directory.join("settings.json"),
            fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert!(Settings::open(directory.clone()).is_err());
        fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn token_lock_password_persistence_and_endpoint_isolation() {
        let directory = std::env::temp_dir().join(format!("skvoz-settings-{}", token().unwrap()));
        let mut settings = Settings::open(directory.clone()).unwrap();
        let id = settings.device("localhost", 4222, "shared").unwrap();
        assert!(Settings::open(directory.clone()).is_err());
        let mut value = settings.value.clone();
        value
            .remember_password("LOCALHOST.", 4222, "shared", "process-test-password".into())
            .unwrap();
        settings.save(value).unwrap();
        assert_eq!(
            fs::metadata(directory.join("settings.json"))
                .unwrap()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(fs::metadata(&directory).unwrap().mode() & 0o777, 0o700);
        drop(settings);
        let mut settings = Settings::open(directory.clone()).unwrap();
        assert_eq!(settings.device("localhost", 4222, "shared").unwrap(), id);
        assert_eq!(
            settings.value.saved_password("LOCALHOST.", 4222, "shared"),
            "process-test-password"
        );
        assert!(
            settings
                .value
                .saved_password("other.example", 4222, "shared")
                .is_empty()
        );
        assert!(
            settings
                .value
                .saved_password("localhost", 4223, "shared")
                .is_empty()
        );
        assert!(
            settings
                .value
                .saved_password("localhost", 4222, "other")
                .is_empty()
        );
        drop(settings);
        fs::remove_dir_all(directory).unwrap();
    }
}
