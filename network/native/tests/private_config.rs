use skvoz_network_native::{
    SecureStateDir, inherit_control, read_private_config, read_root_config,
};
use std::{
    fs,
    os::{
        fd::AsFd,
        unix::fs::{PermissionsExt, symlink},
    },
    path::PathBuf,
    process::Command,
};
// Descriptor-number assertions require no concurrent descriptor allocation in
// this test process; other integration test binaries have independent tables.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("skvoz-private-{}-{stamp}", std::process::id()));
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        Self(path)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
#[test]
fn private_loader_checks_bound_mode_link_and_symlink() {
    let _serial = SERIAL.lock().unwrap();
    let dir = Scratch::new();
    let path = dir.0.join("config.json");
    fs::write(&path, b"private configuration").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        read_private_config(&path, 21).unwrap(),
        b"private configuration"
    );
    assert!(read_private_config(&path, 20).is_err());
    let link = dir.0.join("link.json");
    symlink(&path, &link).unwrap();
    assert!(read_private_config(&link, 100).is_err());
    let hard = dir.0.join("hard.json");
    fs::hard_link(&path, &hard).unwrap();
    assert!(read_private_config(&path, 100).is_err());
    fs::remove_file(hard).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(read_private_config(&path, 100).is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::set_permissions(&dir.0, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(read_private_config(&path, 100).is_err());
    // Root configuration/state never accept a user-writable/sticky ancestor.
    assert!(read_root_config(&path, 100).is_err());
    assert!(SecureStateDir::open(&dir.0).is_err());
}
#[test]
fn multiple_child_mappings_keep_sources_separate_and_parent_owned() {
    let _serial = SERIAL.lock().unwrap();
    let dir = Scratch::new();
    let first = dir.0.join("first");
    let second = dir.0.join("second");
    fs::write(&first, b"first").unwrap();
    fs::write(&second, b"second").unwrap();
    let a = fs::File::open(first).unwrap();
    let b = fs::File::open(second).unwrap();
    let mut command = Command::new("/usr/bin/python3");
    command.args(["-c","import os; print(os.read(3,32).decode(),os.read(4,32).decode()); assert not os.get_inheritable(64) if os.path.exists('/proc/self/fd/64') else True"]);
    inherit_control(&mut command, a.as_fd(), 3).unwrap();
    inherit_control(&mut command, b.as_fd(), 4).unwrap();
    let output = command.output().unwrap();
    drop(command);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"first second\n");
    assert!(a.metadata().is_ok());
    assert!(b.metadata().is_ok());
}

#[test]
fn cli_adoption_closes_original_but_borrowed_duplication_retains_it() {
    let _serial = SERIAL.lock().unwrap();
    use std::os::fd::{AsRawFd, IntoRawFd};
    let original = fs::File::open("/dev/null").unwrap();
    let borrowed = skvoz_network_native::duplicate_cloexec(original.as_fd()).unwrap();
    assert!(original.metadata().is_ok());
    drop(borrowed);
    let raw = original.into_raw_fd();
    let adopted = skvoz_network_native::adopt_inherited(raw).unwrap();
    assert_ne!(adopted.as_raw_fd(), raw);
    assert!(skvoz_network_native::duplicate_inherited(raw).is_err());
    assert!(fs::File::from(adopted).metadata().is_ok());
}
