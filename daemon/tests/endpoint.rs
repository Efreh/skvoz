use skvoz_daemon::{
    config::Profile,
    endpoint::{self, Endpoint},
};
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    sync::atomic::{AtomicU64, Ordering},
};
static SEQUENCE: AtomicU64 = AtomicU64::new(0);
struct Temporary(std::path::PathBuf);
impl Temporary {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "skvoz-endpoint-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&p).unwrap();
        fs::set_permissions(&p, fs::Permissions::from_mode(0o700)).unwrap();
        Self(p)
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
#[tokio::test]
async fn private_endpoint_refuses_takeover_and_preserves_replacement() {
    let t = Temporary::new();
    let uid = endpoint::effective_uid().unwrap();
    let p = t.0.join("core.sock");
    let socket = Endpoint::bind(&p, uid).unwrap();
    assert_eq!(
        fs::metadata(&p).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert!(Endpoint::bind(&p, uid).is_err());
    fs::remove_file(&p).unwrap();
    fs::write(&p, b"replacement").unwrap();
    drop(socket);
    assert_eq!(fs::read(&p).unwrap(), b"replacement");
    assert!(Endpoint::bind(&p, uid).is_err());
    fs::remove_file(&p).unwrap();
    symlink(t.0.join("missing"), &p).unwrap();
    assert!(Endpoint::bind(&p, uid).is_err());
    fs::remove_file(&p).unwrap();
    fs::set_permissions(&t.0, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(Endpoint::bind(&p, uid).is_err());
    // A private leaf under a writable non-sticky ancestor can be renamed by others.
    let child = t.0.join("child");
    fs::create_dir(&child).unwrap();
    fs::set_permissions(&child, fs::Permissions::from_mode(0o700)).unwrap();
    fs::set_permissions(&t.0, fs::Permissions::from_mode(0o777)).unwrap();
    assert!(Endpoint::bind(&child.join("core.sock"), uid).is_err());
}
#[tokio::test]
async fn secret_files_are_private_regular_and_bounded() {
    let t = Temporary::new();
    let uid = endpoint::effective_uid().unwrap();
    let p = t.0.join("secret");
    fs::write(&p, b"12345").unwrap();
    fs::set_permissions(&p, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(endpoint::secret_file(&p, uid, 4).is_err());
    assert_eq!(endpoint::secret_file(&p, uid, 5).unwrap(), b"12345");
    fs::set_permissions(&p, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(endpoint::secret_file(&p, uid, 5).is_err());
    let link = t.0.join("link");
    symlink(&p, &link).unwrap();
    assert!(endpoint::secret_file(&link, uid, 5).is_err());
}
#[tokio::test]
async fn config_checks_types_ranges_unknown_fields_and_redaction() {
    let t = Temporary::new();
    let uid = endpoint::effective_uid().unwrap();
    let p = t.0.join("profile.json");
    let mut profile = serde_json::json!({"ipc_path":t.0.join("core.sock"),"url":"tls://localhost:4222","trust":"system","username":"fixture","password":"fixture","namespace":"example","peer_id":1,"allowed_peers":[0]});
    fs::write(&p, serde_json::to_vec(&profile).unwrap()).unwrap();
    fs::set_permissions(&p, fs::Permissions::from_mode(0o600)).unwrap();
    let loaded = Profile::load(&p, uid).unwrap();
    assert!(!format!("{:?}", loaded.runtime).contains("fixture"));
    profile["unknown"] = serde_json::json!(1);
    fs::write(&p, serde_json::to_vec(&profile).unwrap()).unwrap();
    assert!(Profile::load(&p, uid).is_err());
    profile.as_object_mut().unwrap().remove("unknown");
    profile["limits"] = serde_json::json!({"owners":65});
    fs::write(&p, serde_json::to_vec(&profile).unwrap()).unwrap();
    assert!(Profile::load(&p, uid).is_err());
    profile["limits"] = serde_json::json!({"output_bytes":142});
    fs::write(&p, serde_json::to_vec(&profile).unwrap()).unwrap();
    assert!(Profile::load(&p, uid).is_err());
    profile["limits"] = serde_json::json!({"output_bytes":143});
    fs::write(&p, serde_json::to_vec(&profile).unwrap()).unwrap();
    assert!(Profile::load(&p, uid).is_ok());
    profile["limits"] = serde_json::json!({"receive_window":1,"max_frame":1024});
    fs::write(&p, serde_json::to_vec(&profile).unwrap()).unwrap();
    assert!(Profile::load(&p, uid).is_err());
    for (window, frame, accepted) in [
        (65537, 32768, true),
        (1_048_576, 32768, true),
        (1_048_577, 32768, true),
        (33_554_432, 32768, true),
        (33_554_433, 32768, false),
        (1_048_576, 32769, false),
    ] {
        profile["limits"] = serde_json::json!({"receive_window":window,"max_frame":frame});
        fs::write(&p, serde_json::to_vec(&profile).unwrap()).unwrap();
        assert_eq!(Profile::load(&p, uid).is_ok(), accepted);
    }
    profile["limits"] = serde_json::json!({"receive_window":1_048_576,"max_frame":32768,"receive_bytes_per_peer":65536});
    fs::write(&p, serde_json::to_vec(&profile).unwrap()).unwrap();
    assert!(Profile::load(&p, uid).is_ok());
}
