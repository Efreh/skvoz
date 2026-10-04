//! GTK application lifecycle, backend wiring and desktop event scheduling.
use crate::{
    backend::{Engine, bundled_daemon, run as run_backend},
    proxy::Budgets,
    settings::Settings,
    ui::{View, label, message},
};
use adw::prelude::*;
use gtk::{gio, glib};
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;

pub fn run() -> glib::ExitCode {
    let app = adw::Application::builder()
        .application_id("org.skvoz.Client")
        .flags(gio::ApplicationFlags::empty())
        .build();
    app.add_main_option(
        "background",
        glib::Char::from(0u8),
        glib::OptionFlags::NONE,
        glib::OptionArg::None,
        "Start in the background",
        None,
    );
    let background = std::env::args().any(|arg| arg == "--background");
    let view: Rc<RefCell<Option<Rc<View>>>> = Rc::new(RefCell::new(None));
    let existing = view.clone();
    app.connect_activate(move |app| {
        if let Some(view) = existing.borrow().as_ref() {
            view.window.present();
            return;
        }
        let settings = match Settings::default_path().and_then(Settings::open) {
            Ok(settings) => Arc::new(Mutex::new(settings)),
            Err(error) => {
                let window = adw::ApplicationWindow::builder()
                    .application(app)
                    .title("SKVOZ")
                    .default_width(460)
                    .default_height(220)
                    .build();
                window.set_content(Some(&label(message(error.0), "error")));
                window.present();
                return;
            }
        };
        let daemon = match bundled_daemon() {
            Ok(path) => path,
            Err(_) => return,
        };
        let (tx, rx) = mpsc::channel(8);
        let (status_tx, status_rx) = std::sync::mpsc::sync_channel(32);
        let engine = Engine::new(settings.clone(), daemon, Budgets::default());
        let telemetry = engine.telemetry.clone();
        let resume_watch = crate::desktop::watch_resume(tx.clone());
        crate::desktop::watch_network(tx.clone());
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Client runtime initialization failed");
            runtime.block_on(run_backend(engine, rx, status_tx));
        });
        let built = match View::build_with_telemetry(app, settings, tx.clone(), telemetry) {
            Ok(view) => view,
            Err(_) => return,
        };
        let app_copy = app.clone();
        let hold = app.hold();
        let tray = app
            .dbus_connection()
            .and_then(|bus| crate::tray::Tray::new(bus, &built).ok())
            .map(Rc::new);
        let ui_stats = Rc::downgrade(&built);
        let tray_stats = tray.clone();
        let mut previous = (0, 0, std::time::Instant::now());
        glib::timeout_add_local(Duration::from_secs(1), move || {
            let Some(view) = ui_stats.upgrade() else {
                return glib::ControlFlow::Break;
            };
            let up = view
                .telemetry
                .uploaded
                .load(std::sync::atomic::Ordering::Relaxed);
            let down = view
                .telemetry
                .downloaded
                .load(std::sync::atomic::Ordering::Relaxed);
            let seconds = previous.2.elapsed().as_secs_f64();
            view.speed.set_label(&crate::telemetry::rates(
                down.saturating_sub(previous.1),
                up.saturating_sub(previous.0),
                seconds,
            ));
            previous = (up, down, std::time::Instant::now());
            view.refresh_log();
            if let Some(tray) = &tray_stats {
                tray.update();
            }
            glib::ControlFlow::Continue
        });
        let auto_connect = built.settings_value().is_some_and(|s| s.auto_connect);
        if auto_connect {
            built.button.emit_clicked();
        }
        if background {
            let weak = Rc::downgrade(&built);
            let tray = tray.clone();
            glib::timeout_add_local_once(Duration::from_millis(500), move || {
                if let Some(view) = weak.upgrade()
                    && tray.as_ref().is_none_or(|t| !t.available.get())
                {
                    view.window.present();
                }
            });
        }
        let ui = built.clone();
        glib::timeout_add_local(Duration::from_millis(100), move || {
            let _hold = &hold;
            let _resume_watch = &resume_watch;
            let _tray = &tray;
            for _ in 0..8 {
                match status_rx.try_recv() {
                    Ok(status) => {
                        if status.state == "stopped" {
                            app_copy.quit();
                            return glib::ControlFlow::Break;
                        }
                        ui.apply(&status);
                        if let Some(tray) = &tray {
                            tray.update();
                        }
                    }
                    Err(std::sync::mpsc::TryRecvError::Empty) => break,
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        app_copy.quit();
                        return glib::ControlFlow::Break;
                    }
                }
            }
            glib::ControlFlow::Continue
        });
        if !background {
            built.window.present();
        }
        *existing.borrow_mut() = Some(built);
    });
    let code = app.run();
    if let Some(view) = view.borrow_mut().take() {
        let dialog = view.settings_window.borrow_mut().take();
        if let Some(log) = view.log_window.borrow_mut().take() {
            log.destroy();
        }
        let window = view.window.clone();
        drop(view);
        if let Some(dialog) = dialog {
            dialog.destroy();
        }
        window.destroy();
    }
    code
}
