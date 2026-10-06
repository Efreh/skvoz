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

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Proxy,
    Vpn,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Preferences {
    pub v: u8,
    pub mode: Mode,
    pub families: Vec<u8>,
    pub max_mtu: u16,
    pub transport_snapshots: BTreeMap<String, String>,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub http_port: u16,
    pub socks_port: u16,
    pub ca_file: String,
    pub devices: BTreeMap<String, String>,
    pub passwords: BTreeMap<String, String>,
    pub autostart: bool,
    pub auto_connect: bool,
    pub tray_speed: bool,
    pub request_log: bool,
}
impl Default for Preferences {
    fn default() -> Self {
        Self {
            v: 2,
            mode: Mode::Proxy,
            families: Vec::new(),
            max_mtu: 1500,
            transport_snapshots: BTreeMap::new(),
            host: String::new(),
            port: 4222,
            username: String::new(),
            http_port: 8080,
            socks_port: 1080,
            ca_file: String::new(),
            devices: BTreeMap::new(),
            passwords: BTreeMap::new(),
            autostart: false,
            auto_connect: false,
            tray_speed: false,
            request_log: true,
        }
    }
}

impl Preferences {
    pub fn runtime_families(&self) -> Vec<u8> {
        if self.families.is_empty() {
            if self.max_mtu >= 1280 {
                vec![4, 6]
            } else {
                vec![4]
            }
        } else {
            self.families.clone()
        }
    }
    pub fn ip_request(&self) -> skvoz_network::local_api::IpArgs {
        skvoz_network::local_api::IpArgs {
            families: self.runtime_families(),
            family_policy: if self.families.is_empty() {
                skvoz_network::FamilyPolicy::Auto
            } else {
                skvoz_network::FamilyPolicy::RequireAll
            },
            max_mtu: self.max_mtu,
            channels: 1,
        }
    }
    pub fn validate_network(&self) -> Result<()> {
        if self.v != 2
            || !matches!(self.families.as_slice(), [] | [4] | [6] | [4, 6])
            || !(576..=1500).contains(&self.max_mtu)
            || self.families.contains(&6) && self.max_mtu < 1280
        {
            return Err(Error("invalid_network_settings"));
        }
        if self.transport_snapshots.len() > 64
            || self.transport_snapshots.iter().any(|(key, ip)| {
                let Ok((host, port, user, device, mode)) =
                    serde_json::from_str::<(String, u16, String, String, String)>(key)
                else {
                    return true;
                };
                port == 0
                    || host_name(&host).is_ok_and(|h| h != host)
                    || host_name(&host).is_err()
                    || !login(&user)
                    || !valid_token(&device)
                    || mode != "vpn"
                    || ip.parse::<IpAddr>().is_err()
                    || ip.parse::<IpAddr>().is_ok_and(|i| {
                        i.to_string() != *ip
                            || i.is_unspecified()
                            || i.is_multicast()
                            || i.to_canonical() != i
                    })
            })
        {
            return Err(Error("unsafe_settings"));
        }
        Ok(())
    }
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

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DisplayPreferences {
    pub v: u8,
    pub speed_unit: crate::telemetry::SpeedUnit,
}
impl Default for DisplayPreferences {
    fn default() -> Self {
        Self {
            v: 1,
            speed_unit: crate::telemetry::SpeedUnit::default(),
        }
    }
}
pub struct Settings {
    pub directory: PathBuf,
    pub value: Preferences,
    pub display: DisplayPreferences,
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
        if value.v != 2
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
        value.validate_network()?;
        ports(value.http_port, value.socks_port)?;
        let display_path = directory.join("ui.json");
        let display: DisplayPreferences = if display_path.exists() || display_path.is_symlink() {
            serde_json::from_slice(&read_private(&display_path, 1024)?)
                .map_err(|_| Error("unsafe_settings"))?
        } else {
            DisplayPreferences::default()
        };
        if display.v != 1 {
            return Err(Error("unsafe_settings"));
        }
        Ok(Self {
            directory,
            value,
            display,
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
        candidate.validate_network()?;
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
    pub fn save_display(&mut self, candidate: DisplayPreferences) -> Result<()> {
        if candidate.v != 1 {
            return Err(Error("unsafe_settings"));
        }
        let bytes = serde_json::to_vec(&candidate).map_err(|_| Error("unsafe_settings"))?;
        write_private(&self.directory.join("ui.json"), &bytes)?;
        self.display = candidate;
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
    fn automatic_default_offers_only_mtu_compatible_families_and_preserves_manual_selection() {
        let mut value = Preferences::default();
        assert!(value.families.is_empty());
        value.validate_network().unwrap();
        let request = value.ip_request();
        assert_eq!(request.family_policy, skvoz_network::FamilyPolicy::Auto);
        assert_eq!(request.families, [4, 6]);
        value.max_mtu = 1279;
        value.validate_network().unwrap();
        assert_eq!(value.ip_request().families, [4]);
        for families in [vec![4], vec![6], vec![4, 6]] {
            value.max_mtu = 1500;
            value.families = families.clone();
            let stored = serde_json::to_vec(&value).unwrap();
            let restored: Preferences = serde_json::from_slice(&stored).unwrap();
            restored.validate_network().unwrap();
            assert_eq!(restored.ip_request().families, families);
            assert_eq!(
                restored.ip_request().family_policy,
                skvoz_network::FamilyPolicy::RequireAll
            );
        }
    }
    #[test]
    fn network_settings_reject_unusable_families_and_unbound_transport_cache() {
        let mut value = Preferences {
            families: vec![4, 6],
            max_mtu: 1279,
            ..Preferences::default()
        };
        assert!(value.validate_network().is_err());
        value.families = vec![4];
        assert!(value.validate_network().is_ok());
        value.families = vec![6, 4];
        assert!(value.validate_network().is_err());
        value.families = vec![4, 6];
        value.max_mtu = 1500;
        let key =
            serde_json::to_string(&("example.org", 4222, "owner", "a".repeat(32), "vpn")).unwrap();
        value
            .transport_snapshots
            .insert(key.clone(), "203.0.113.5".into());
        assert!(value.validate_network().is_ok());
        value
            .transport_snapshots
            .insert(key, "::ffff:203.0.113.5".into());
        assert!(value.validate_network().is_err());
        value.transport_snapshots.clear();
        value
            .transport_snapshots
            .insert("[\"example.org\",4222]".into(), "203.0.113.5".into());
        assert!(value.validate_network().is_err());
        value.transport_snapshots.clear();
        value.v = 1;
        assert!(value.validate_network().is_err());
    }
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
    fn display_units_persist_privately_without_changing_connection_settings() {
        let directory = std::env::temp_dir().join(format!("skvoz-display-{}", token().unwrap()));
        let mut settings = Settings::open(directory.clone()).unwrap();
        settings.save(settings.value.clone()).unwrap();
        let connection = fs::read(directory.join("settings.json")).unwrap();
        assert_eq!(
            settings.display.speed_unit,
            crate::telemetry::SpeedUnit::Kibibytes
        );
        settings
            .save_display(DisplayPreferences {
                v: 1,
                speed_unit: crate::telemetry::SpeedUnit::Megabits,
            })
            .unwrap();
        assert_eq!(
            fs::read(directory.join("settings.json")).unwrap(),
            connection
        );
        assert_eq!(
            fs::metadata(directory.join("ui.json")).unwrap().mode() & 0o777,
            0o600
        );
        drop(settings);
        let settings = Settings::open(directory.clone()).unwrap();
        assert_eq!(
            settings.display.speed_unit,
            crate::telemetry::SpeedUnit::Megabits
        );
        drop(settings);
        for bytes in [
            br#"{"v":1}"#.as_slice(),
            br#"{"v":2,"speed_unit":"megabits"}"#,
            br#"{"v":1,"speed_unit":"auto"}"#,
            br#"{"v":1,"speed_unit":"megabits","extra":true}"#,
        ] {
            write_private(&directory.join("ui.json"), bytes).unwrap();
            assert!(Settings::open(directory.clone()).is_err());
        }
        fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn current_settings_and_private_secret_validation() {
        let directory = std::env::temp_dir().join(format!("skvoz-settings-{}", token().unwrap()));
        let settings = Settings::open(directory.clone()).unwrap();
        let current = serde_json::to_value(&settings.value).unwrap();
        drop(settings);
        for name in [
            "mode",
            "families",
            "max_mtu",
            "transport_snapshots",
            "passwords",
            "autostart",
            "auto_connect",
            "tray_speed",
            "request_log",
        ] {
            let mut incomplete = current.clone();
            incomplete.as_object_mut().unwrap().remove(name);
            write_private(
                &directory.join("settings.json"),
                &serde_json::to_vec(&incomplete).unwrap(),
            )
            .unwrap();
            assert!(Settings::open(directory.clone()).is_err());
        }
        write_private(
            &directory.join("settings.json"),
            &serde_json::to_vec(&current).unwrap(),
        )
        .unwrap();
        let mut settings = Settings::open(directory.clone()).unwrap();
        assert!(settings.value.passwords.is_empty());
        assert!(
            !settings.value.autostart && !settings.value.auto_connect && !settings.value.tray_speed
        );
        assert!(settings.value.request_log);
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
