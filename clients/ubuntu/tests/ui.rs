use adw::prelude::*;
use skvoz_ubuntu_client::{
    backend::{Control, Engine, run},
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
fn dbus_call(
    bus: &gtk::gio::DBusConnection,
    path: &str,
    interface: &str,
    method: &str,
    parameters: &gtk::glib::Variant,
) -> gtk::glib::Variant {
    gtk::glib::MainContext::default()
        .block_on(bus.call_future(
            bus.unique_name().as_deref(),
            path,
            interface,
            method,
            Some(parameters),
            None,
            gtk::gio::DBusCallFlags::NONE,
            2000,
        ))
        .unwrap()
}
fn qualify_desktop(view: &std::rc::Rc<View>) {
    use glib::variant::ToVariant;
    use gtk::{gio, glib};
    use std::{cell::RefCell, collections::BTreeMap, rc::Rc};
    let bus = gio::bus_get_sync(gio::BusType::Session, None::<&gio::Cancellable>).unwrap();
    let registered = Rc::new(RefCell::new(None));
    let received = registered.clone();
    let sender_name = Rc::new(RefCell::new(String::new()));
    let sender = sender_name.clone();
    let node = gio::DBusNodeInfo::for_xml("<node><interface name='org.kde.StatusNotifierWatcher'><method name='RegisterStatusNotifierItem'><arg type='s' direction='in'/></method></interface></node>").unwrap();
    let host = bus
        .register_object(
            "/StatusNotifierWatcher",
            &node
                .lookup_interface("org.kde.StatusNotifierWatcher")
                .unwrap(),
        )
        .method_call(move |_, name, _, _, _, args, invocation| {
            *sender.borrow_mut() = name.unwrap_or_default().to_owned();
            *received.borrow_mut() = args.get::<(String,)>();
            invocation.return_value(Some(&().to_variant()));
        })
        .build()
        .unwrap();
    let owner = gio::bus_own_name_on_connection(
        &bus,
        "org.kde.StatusNotifierWatcher",
        gio::BusNameOwnerFlags::NONE,
        |_, _| {},
        |_, _| {},
    );
    let tray = skvoz_ubuntu_client::tray::Tray::new(bus.clone(), view).unwrap();
    tick(Duration::from_millis(300));
    assert!(tray.available.get());
    assert_eq!(
        registered.borrow().as_ref().unwrap().0,
        "/StatusNotifierItem"
    );
    // GNOME renders XAyatanaLabel as plain text and ignores its width guide.
    let panel = gtk::Label::new(None);
    let mut panel_widths = std::collections::BTreeMap::new();
    let mut window_width = None;
    for (down, up) in [
        (0, 0),
        (307, 30),
        (1024, 1023),
        (1_000_000, 99_999_999),
        (1_023_999_980, 1024),
        (1_024_000_000, u64::MAX),
    ] {
        let text = skvoz_ubuntu_client::telemetry::rates(down, up, 1.0);
        view.speed.set_text(&text);
        tray.update();
        let prop = dbus_call(
            &bus,
            "/StatusNotifierItem",
            "org.freedesktop.DBus.Properties",
            "Get",
            &("org.kde.StatusNotifierItem", "XAyatanaLabel").to_variant(),
        );
        assert_eq!(
            prop.child_value(0)
                .as_variant()
                .unwrap()
                .get::<String>()
                .unwrap(),
            text
        );
        tick(Duration::from_millis(50));
        for font in ["Sans 11", "Ubuntu Sans 11", "Ubuntu 11"] {
            let layout = panel.create_pango_layout(Some(&text));
            layout.set_font_description(Some(&gtk::pango::FontDescription::from_string(font)));
            let width = layout.pixel_size().0;
            assert_eq!(
                *panel_widths.entry(font).or_insert(width),
                width,
                "panel {font}: {text:?}"
            );
        }
        let width = view.speed.width();
        assert_eq!(
            *window_width.get_or_insert(width),
            width,
            "window: {text:?}"
        );
    }
    view.speed
        .set_text(&skvoz_ubuntu_client::telemetry::rates(0, 0, 1.0));
    let reply = dbus_call(
        &bus,
        "/Menu",
        "com.canonical.dbusmenu",
        "GetLayout",
        &(0i32, -1i32, Vec::<String>::new()).to_variant(),
    );
    assert_eq!(reply.type_().as_str(), "(u(ia{sv}av))");
    let root = reply
        .child_value(1)
        .get::<(i32, BTreeMap<String, glib::Variant>, Vec<glib::Variant>)>()
        .unwrap();
    assert_eq!(root.2.len(), 7);
    let names = root
        .2
        .iter()
        .map(|item| {
            item.get::<(i32, BTreeMap<String, glib::Variant>, Vec<glib::Variant>)>()
                .unwrap()
                .1["label"]
                .get::<String>()
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(names.contains(&"Показать SKVOZ".into()));
    assert!(names.contains(&"Завершить SKVOZ".into()));
    view.window.close();
    tick(Duration::from_millis(50));
    assert!(!view.window.is_visible());
    dbus_call(
        &bus,
        "/Menu",
        "com.canonical.dbusmenu",
        "Event",
        &(1i32, "clicked", 0i32.to_variant(), 0u32).to_variant(),
    );
    assert!(view.window.is_visible());
    dbus_call(
        &bus,
        "/Menu",
        "com.canonical.dbusmenu",
        "Event",
        &(5i32, "clicked", 0i32.to_variant(), 0u32).to_variant(),
    );
    assert!(view.log_window.borrow().as_ref().unwrap().is_visible());
    let mut flow = view.telemetry.flow();
    flow.destination("CONNECT", "example.org", 443);
    flow.opened();
    flow.upload(1024);
    flow.download(2048);
    flow.finish(Ok(()));
    drop(flow);
    view.refresh_log();
    let preview = view.preview.buffer();
    assert!(
        preview
            .text(&preview.start_iter(), &preview.end_iter(), false)
            .contains("example.org:443")
    );
    let log = view.log_window.borrow().as_ref().unwrap().clone();
    let text = widgets(log.upcast_ref())
        .into_iter()
        .filter_map(|w| w.downcast::<gtk::TextView>().ok())
        .next()
        .unwrap();
    let buffer = text.buffer();
    let contents = buffer.text(&buffer.start_iter(), &buffer.end_iter(), false);
    assert!(contents.contains("CONNECT  example.org:443") && contents.contains("1024 Б"));
    view.telemetry.enable(false);
    view.refresh_log();
    assert!(
        !buffer
            .text(&buffer.start_iter(), &buffer.end_iter(), false)
            .contains("example.org")
    );
    if let Some(path) = std::env::var_os("SKVOZ_LOG_XWD") {
        view.telemetry.enable(true);
        let mut flow = view.telemetry.flow();
        flow.destination("HTTP", "example.org", 80);
        flow.opened();
        flow.upload(1234);
        flow.download(45678);
        flow.finish(Ok(()));
        drop(flow);
        view.refresh_log();
        tick(Duration::from_millis(200));
        assert!(
            std::process::Command::new("xwd")
                .args(["-silent", "-root", "-out"])
                .arg(path)
                .status()
                .unwrap()
                .success()
        );
    }
    log.close();
    tick(Duration::from_millis(50));
    assert!(view.log_window.borrow().is_none());
    tray.update();
    let prop = dbus_call(
        &bus,
        "/StatusNotifierItem",
        "org.freedesktop.DBus.Properties",
        "Get",
        &("org.kde.StatusNotifierItem", "IconName").to_variant(),
    );
    assert_eq!(
        prop.child_value(0)
            .as_variant()
            .unwrap()
            .get::<String>()
            .unwrap(),
        "network-offline-symbolic"
    );
    // Re-registration after an indicator host restart uses the same item path.
    gio::bus_unown_name(owner);
    tick(Duration::from_millis(100));
    assert!(!tray.available.get());
    let owner = gio::bus_own_name_on_connection(
        &bus,
        "org.kde.StatusNotifierWatcher",
        gio::BusNameOwnerFlags::NONE,
        |_, _| {},
        |_, _| {},
    );
    tick(Duration::from_millis(200));
    assert!(tray.available.get());
    let directory =
        std::env::temp_dir().join(format!("skvoz-desktop-process-{}", token().unwrap()));
    let product = std::env::var_os("SKVOZ_TEST_GUI")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_skvoz-client").into());
    let mut child = std::process::Command::new(&product)
        .arg("--background")
        .env("XDG_CONFIG_HOME", &directory)
        .env("GTK_A11Y", "none")
        .spawn()
        .unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    let our_name = bus.unique_name().unwrap().to_string();
    while Instant::now() < end && *sender_name.borrow() == our_name {
        tick(Duration::from_millis(50));
    }
    let product_name = sender_name.borrow().clone();
    assert_ne!(product_name, our_name);
    tick(Duration::from_millis(800));
    assert!(
        child.try_wait().unwrap().is_none(),
        "Hidden standalone application must stay running"
    );
    // A second launch activates the existing product instance and exits.
    let mut second = std::process::Command::new(&product)
        .env("XDG_CONFIG_HOME", &directory)
        .env("GTK_A11Y", "none")
        .spawn()
        .unwrap();
    let end = Instant::now() + Duration::from_secs(3);
    while Instant::now() < end && second.try_wait().unwrap().is_none() {
        tick(Duration::from_millis(50));
    }
    assert!(second.try_wait().unwrap().unwrap().success());
    assert!(child.try_wait().unwrap().is_none());
    glib::MainContext::default()
        .block_on(bus.call_future(
            Some(&product_name),
            "/Menu",
            "com.canonical.dbusmenu",
            "Event",
            Some(&(7i32, "clicked", 0i32.to_variant(), 0u32).to_variant()),
            None,
            gio::DBusCallFlags::NONE,
            2000,
        ))
        .unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    while Instant::now() < end && child.try_wait().unwrap().is_none() {
        tick(Duration::from_millis(50));
    }
    assert!(child.try_wait().unwrap().unwrap().success());
    std::fs::remove_dir_all(directory).unwrap();
    drop(tray);
    gio::bus_unown_name(owner);
    bus.unregister_object(host).unwrap();
    // Exercise the real GIO signal subscription without suspending the machine.
    let owner = gio::bus_own_name_on_connection(
        &bus,
        "org.freedesktop.login1",
        gio::BusNameOwnerFlags::NONE,
        |_, _| {},
        |_, _| {},
    );
    tick(Duration::from_millis(100));
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let subscription = skvoz_ubuntu_client::desktop::resume_subscription(&bus, tx);
    tick(Duration::from_millis(100));
    bus.emit_signal(
        None,
        "/org/freedesktop/login1",
        "org.freedesktop.login1.Manager",
        "PrepareForSleep",
        Some(&(true,).to_variant()),
    )
    .unwrap();
    tick(Duration::from_millis(50));
    assert!(rx.try_recv().is_err());
    bus.emit_signal(
        None,
        "/org/freedesktop/login1",
        "org.freedesktop.login1.Manager",
        "PrepareForSleep",
        Some(&(false,).to_variant()),
    )
    .unwrap();
    tick(Duration::from_millis(50));
    assert!(matches!(rx.try_recv(), Ok(Control::Resume)));
    drop(subscription);
    gio::bus_unown_name(owner);
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
    {
        let mut settings = settings.lock().unwrap();
        let mut value = settings.value.clone();
        value.host = "localhost".into();
        value.username = "shared".into();
        value.tray_speed = true;
        value
            .remember_password("localhost", 4222, "shared", "saved-test-password".into())
            .unwrap();
        settings.save(value).unwrap();
    }
    let app = adw::Application::builder()
        .application_id("org.skvoz.Qualification")
        .flags(gtk::gio::ApplicationFlags::NON_UNIQUE)
        .build();
    app.register(None::<&gtk::gio::Cancellable>).unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let (status_tx, status_rx) = std::sync::mpsc::sync_channel(32);
    let engine = Engine::new(settings.clone(), "/does-not-exist".into());
    let worker = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(run(engine, rx, status_tx));
    });
    let view = View::build(&app, settings.clone(), tx.clone()).unwrap();
    assert_eq!(view.password.text(), "saved-test-password");
    view.port.set_text("4223");
    assert!(view.password.text().is_empty());
    view.port.set_text("4222");
    assert_eq!(view.password.text(), "saved-test-password");
    view.window.present();
    tick(Duration::from_millis(400));
    assert_eq!(view.status.text(), "Отключено");
    assert!(widgets(view.window.upcast_ref()).into_iter().any(|widget| {
        widget.downcast::<gtk::Label>().is_ok_and(|label| {
            label.text() == format!("Версия {}", env!("CARGO_PKG_VERSION")) && label.is_visible()
        })
    }));
    qualify_desktop(&view);
    if let Some(path) = std::env::var_os("SKVOZ_MAIN_XWD") {
        tick(Duration::from_millis(200));
        assert!(
            std::process::Command::new("xwd")
                .args(["-silent", "-root", "-out"])
                .arg(path)
                .status()
                .unwrap()
                .success()
        );
    }
    assert_eq!(view.http.subtitle().unwrap(), "http://127.0.0.1:8080");
    for row in [&view.http, &view.socks] {
        let button = widgets(row.upcast_ref())
            .into_iter()
            .filter_map(|widget| widget.downcast::<gtk::Button>().ok())
            .find(|button| {
                button
                    .tooltip_text()
                    .is_some_and(|text| text == "Скопировать адрес")
            })
            .unwrap();
        button.emit_clicked();
        let text = gtk::glib::MainContext::default()
            .block_on(button.clipboard().read_text_future())
            .unwrap()
            .unwrap();
        assert_eq!(text, row.subtitle().unwrap());
    }
    view.show_settings();
    tick(Duration::from_millis(200));
    let dialog = view.settings_window.borrow().clone().unwrap();
    let all = widgets(dialog.upcast_ref());
    let mode = all
        .iter()
        .filter_map(|widget| widget.clone().downcast::<adw::ComboRow>().ok())
        .find(|row| row.title() == "Режим")
        .unwrap();
    let choices = mode.model().unwrap().downcast::<gtk::StringList>().unwrap();
    assert_eq!(choices.n_items(), 2);
    assert_eq!(choices.string(0).unwrap(), "Прокси");
    assert_eq!(choices.string(1).unwrap(), "ВПН");
    let rows: Vec<adw::EntryRow> = all
        .iter()
        .filter_map(|widget| widget.clone().downcast::<adw::EntryRow>().ok())
        .filter(|row| ["HTTP / HTTPS", "SOCKS5"].contains(&row.title().as_str()))
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
    tick(Duration::from_millis(1600));
    let copy = widgets(view.http.upcast_ref())
        .into_iter()
        .filter_map(|w| w.downcast::<gtk::Button>().ok())
        .find(|b| b.tooltip_text().is_some_and(|t| t == "Скопировать адрес"))
        .unwrap();
    copy.emit_clicked();
    assert_eq!(
        gtk::glib::MainContext::default()
            .block_on(copy.clipboard().read_text_future())
            .unwrap()
            .unwrap(),
        "http://127.0.0.1:8181"
    );
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
    assert_eq!(view.password.text(), "process-test-password");
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
