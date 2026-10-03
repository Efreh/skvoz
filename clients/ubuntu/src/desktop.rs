//! Desktop integration uses the existing GIO runtime and XDG conventions.
use crate::{Error, Result, backend::Control};
use gtk::gio::{self, prelude::*};
use std::{
    fs,
    io::Write,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};
use tokio::sync::mpsc;
pub fn autostart(base: &Path, enabled: bool) -> Result<()> {
    let directory = base.join("autostart");
    fs::create_dir_all(&directory)?;
    for parent in directory.ancestors() {
        let meta = fs::symlink_metadata(parent)?;
        if !meta.is_dir()
            || ![0, crate::settings::uid()?].contains(&meta.uid())
            || meta.mode() & 0o022 != 0 && meta.mode() & 0o1000 == 0
        {
            return Err(Error("unsafe_settings"));
        }
    }
    let path = directory.join("org.skvoz.Client.desktop");
    if !enabled {
        if path.exists() || path.is_symlink() {
            fs::remove_file(&path)?;
        }
        return Ok(());
    }
    let temporary = directory.join(format!(".skvoz-{}", crate::settings::token()?));
    let result = (|| {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&temporary)?;
        file.write_all(b"[Desktop Entry]\nType=Application\nName=SKVOZ\nExec=skvoz-client --background\nIcon=org.skvoz.Client\nTerminal=false\nX-GNOME-Autostart-enabled=true\n")?;
        file.sync_all()?;
        fs::rename(&temporary, &path)?;
        Ok(())
    })();
    let _ = fs::remove_file(temporary);
    result
}
/// Resume is a notification, never a sleep inhibitor. No cleanup is assumed
/// to complete before suspension; all stale streams retire after wake-up.
pub fn resume_subscription(
    bus: &gio::DBusConnection,
    commands: mpsc::Sender<Control>,
) -> gio::SignalSubscription {
    bus.subscribe_to_signal(
        Some("org.freedesktop.login1"),
        Some("org.freedesktop.login1.Manager"),
        Some("PrepareForSleep"),
        Some("/org/freedesktop/login1"),
        None,
        gio::DBusSignalFlags::NONE,
        move |signal| {
            if signal.parameters.get::<(bool,)>() == Some((false,)) {
                let _ = commands.try_send(Control::Resume);
            }
        },
    )
}
pub fn watch_resume(
    commands: mpsc::Sender<Control>,
) -> std::rc::Rc<std::cell::RefCell<Option<gio::SignalSubscription>>> {
    let watch = std::rc::Rc::new(std::cell::RefCell::new(None));
    let weak = std::rc::Rc::downgrade(&watch);
    gio::bus_get(
        gio::BusType::System,
        None::<&gio::Cancellable>,
        move |result| {
            if let (Ok(bus), Some(watch)) = (result, weak.upgrade()) {
                *watch.borrow_mut() = Some(resume_subscription(&bus, commands));
            }
        },
    );
    watch
}
pub fn watch_network(commands: mpsc::Sender<Control>) {
    let monitor = gio::NetworkMonitor::default();
    monitor.connect_network_changed(move |_, available| {
        if available {
            let _ = commands.try_send(Control::NetworkAvailable);
        }
    });
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn autostart_is_opt_in_and_contains_no_credentials() {
        let directory =
            std::env::temp_dir().join(format!("skvoz-xdg-{}", crate::settings::token().unwrap()));
        autostart(&directory, true).unwrap();
        let text =
            fs::read_to_string(directory.join("autostart/org.skvoz.Client.desktop")).unwrap();
        assert!(text.contains("Exec=skvoz-client --background"));
        assert!(!text.contains("password"));
        autostart(&directory, false).unwrap();
        assert!(
            !directory
                .join("autostart/org.skvoz.Client.desktop")
                .exists()
        );
        fs::remove_dir_all(directory).unwrap();
    }
}
