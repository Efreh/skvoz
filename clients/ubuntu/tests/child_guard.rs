//! Real fork/exec qualification of the supervised child's parent and control descriptor.
use std::{
    fs,
    io::Read,
    os::{
        fd::AsFd,
        unix::{fs::PermissionsExt, net::UnixStream},
    },
    path::PathBuf,
    process::{Child, Command},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
struct Owner(Child);
impl Drop for Owner {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn guard_exec_preserves_pid_arguments_and_control_fd_and_denies_wrong_parent() {
    let directory = Fixture(std::env::temp_dir().join(format!(
            "skvoz-child-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )));
    fs::create_dir(&directory.0).unwrap();
    let executable = directory.0.join("runtime-probe");
    fs::write(
        &executable,
        "#!/bin/sh\nprintf '%s\\n' \"$#\" \"$1\" \"$2\" \"$3\" \"$4\" \"$$\" >&3\n",
    )
    .unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
    let profile = directory.0.join("profile with spaces.json");
    fs::write(&profile, "{}").unwrap();
    for invalid in [None, Some("parent"), Some("extra")] {
        let (mut local, remote) = UnixStream::pair().unwrap();
        local
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_skvoz-client-test-driver"));
        command
            .arg("--child")
            .arg(if invalid == Some("parent") {
                "0".into()
            } else {
                std::process::id().to_string()
            })
            .arg(&executable)
            .arg(&profile)
            .arg("3");
        if invalid == Some("extra") {
            command.arg("unexpected");
        }
        // Use the same borrowed control-FD inheritance boundary as the product launcher.
        skvoz_network_native::inherit_control(&mut command, remote.as_fd(), 3).unwrap();
        let mut owner = Owner(command.spawn().unwrap());
        drop(command); // Release the captured parent duplicate before waiting for EOF.
        drop(remote);
        let mut output = String::new();
        local.read_to_string(&mut output).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        let status = loop {
            if let Some(status) = owner.0.try_wait().unwrap() {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "Supervised child did not terminate"
            );
            std::thread::sleep(Duration::from_millis(5));
        };
        if invalid.is_none() {
            assert!(status.success());
            assert_eq!(
                output,
                format!(
                    "4\n--config\n{}\n--control-fd\n3\n{}\n",
                    profile.display(),
                    owner.0.id()
                )
            );
        } else {
            assert!(!status.success());
            assert!(
                output.is_empty(),
                "Invalid guard attempt executed the target"
            );
        }
    }
}
