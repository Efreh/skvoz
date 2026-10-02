use adw::prelude::*;
use skvoz_ubuntu_client::{
    backend::{Control, Engine, run},
    proxy::Budgets,
    settings::{Settings, token},
    ui::View,
};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
fn tick(duration: Duration) {
    let end = Instant::now() + duration;
    while Instant::now() < end {
        while gtk::glib::MainContext::default().pending() {
            gtk::glib::MainContext::default().iteration(false);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn widgets(root: &gtk::Widget) -> Vec<gtk::Widget> {
    let mut output = vec![root.clone()];
    let mut child = root.first_child();
    while let Some(current) = child {
        output.extend(widgets(&current));
        child = current.next_sibling();
    }
    output
}
#[test]
fn native_window_settings_and_backend_error() {
    if std::env::var_os("SKVOZ_UI_TEST").is_none() {
        eprintln!("UI qualification requires SKVOZ_UI_TEST=1 and a display");
        return;
    }
    gtk::init().expect("GTK display initialization failed");
    adw::init().expect("Adwaita initialization failed");
    let directory = std::env::temp_dir().join(format!("skvoz-ui-{}", token().unwrap()));
    let settings = Arc::new(Mutex::new(Settings::open(directory.clone()).unwrap()));
    let app = adw::Application::builder()
        .application_id("org.skvoz.Qualification")
        .flags(gtk::gio::ApplicationFlags::NON_UNIQUE)
        .build();
    app.register(None::<&gtk::gio::Cancellable>).unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let (status_tx, status_rx) = std::sync::mpsc::sync_channel(32);
    let engine = Engine::new(
        settings.clone(),
        "/does-not-exist".into(),
        Budgets::default(),
    );
    let worker = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(run(engine, rx, status_tx));
    });
    let view = View::build(&app, settings.clone(), tx.clone()).unwrap();
    view.window.present();
    tick(Duration::from_millis(400));
    assert_eq!(view.status.text(), "Отключено");
    assert_eq!(view.http.subtitle().unwrap(), "http://127.0.0.1:8080");
    view.show_settings();
    tick(Duration::from_millis(200));
    let dialog = view.settings_window.borrow().clone().unwrap();
    let all = widgets(dialog.upcast_ref());
    let rows: Vec<adw::EntryRow> = all
        .iter()
        .filter_map(|widget| widget.clone().downcast::<adw::EntryRow>().ok())
        .filter(|row| ["HTTP / HTTPS CONNECT", "SOCKS5 CONNECT"].contains(&row.title().as_str()))
        .collect();
    assert_eq!(rows.len(), 2);
    rows[0].set_text("invalid");
    let save = all
        .iter()
        .filter_map(|widget| widget.clone().downcast::<gtk::Button>().ok())
        .find(|button| button.label().is_some_and(|text| text == "Сохранить"))
        .unwrap();
    save.emit_by_name::<()>("clicked", &[]);
    tick(Duration::from_millis(100));
    assert!(
        widgets(dialog.upcast_ref())
            .iter()
            .filter_map(|widget| widget.clone().downcast::<gtk::Label>().ok())
            .any(|label| label.is_visible() && label.text().contains("допустимый порт"))
    );
    rows[0].set_text("8181");
    rows[1].set_text("1181");
    save.emit_by_name::<()>("clicked", &[]);
    tick(Duration::from_millis(100));
    assert_eq!(settings.lock().unwrap().value.http_port, 8181);
    view.host.set_text("invalid host");
    view.username.set_text("shared");
    view.password.set_text("process-test-password");
    view.button.emit_by_name::<()>("clicked", &[]);
    let end = Instant::now() + Duration::from_secs(5);
    while Instant::now() < end {
        tick(Duration::from_millis(50));
        while let Ok(status) = status_rx.try_recv() {
            view.apply(&status);
        }
        if view.state.get() == "error" {
            break;
        }
    }
    assert_eq!(view.state.get(), "error");
    assert!(view.error.is_visible());
    assert!(view.error.text().contains("IP-адрес"));
    assert!(view.password.text().is_empty());
    tick(Duration::from_millis(400));
    if let Some(path) = std::env::var_os("SKVOZ_UI_XWD") {
        assert!(
            std::process::Command::new("xwd")
                .args(["-silent", "-root", "-out"])
                .arg(path)
                .status()
                .unwrap()
                .success()
        );
    }
    tx.blocking_send(Control::Quit).unwrap();
    worker.join().unwrap();
    drop(rows);
    drop(save);
    drop(all);
    drop(dialog);
    let window = view.window.clone();
    let weak = std::rc::Rc::downgrade(&view);
    drop(view);
    assert!(weak.upgrade().is_none());
    window.destroy();
    drop(settings);
    drop(Settings::open(directory.clone()).unwrap());
    std::fs::remove_dir_all(directory).unwrap();
}
