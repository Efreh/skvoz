use crate::{
    Error, Result,
    backend::{Control, Engine, SharedSettings, Status, bundled_daemon, run},
    proxy::Budgets,
    settings::{Settings, port},
};
use adw::prelude::*;
use gtk::{gio, glib};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;
pub fn message(code: &str) -> &'static str {
    match code {
        "invalid_address" => "Проверьте IP-адрес или имя сервера.",
        "invalid_port" => "Укажите допустимый порт. Локальный: 1024–65535; серверный: 1–65535.",
        "port_conflict" => "HTTP и SOCKS5 должны использовать разные порты.",
        "port_busy" => "Локальный порт занят. Измените его в настройках.",
        "invalid_login" => "Логин: латинские буквы, цифры, дефис или подчёркивание.",
        "invalid_password" => "Пароль должен содержать от 12 до 72 байт UTF-8.",
        "authentication_failed" => "Сервер отклонил логин или пароль.",
        "certificate_failed" => "Сертификат сервера не прошёл проверку доверия или имени.",
        "invalid_ca" => "Не удалось прочитать сертификат частного центра сертификации.",
        "server_unavailable" => "Сервер недоступен. Проверьте адрес, порт и сеть.",
        "enrollment_failed" => {
            "Сервер не смог зарегистрировать устройство. Проверьте версию и повторите подключение."
        }
        "device_limit" => "Достигнут лимит устройств. Обратитесь к администратору сервера.",
        "version_mismatch" => "Версии клиента и серверного протокола несовместимы.",
        "unsafe_settings" => "Файлы настроек имеют недопустимые права или формат.",
        "already_running" => "Это устройство уже используется другим экземпляром SKVOZ.",
        "already_connected" => "Сначала отключитесь, чтобы изменить настройки.",
        _ => "Не удалось запустить ядро. Проверьте установку приложения.",
    }
}
fn state_name(state: &str) -> &'static str {
    match state {
        "connecting" => "Подключение…",
        "connected" => "Подключено",
        "reconnecting" => "Восстановление соединения…",
        "error" => "Не удалось подключиться",
        _ => "Отключено",
    }
}
#[derive(Clone)]
pub struct View {
    pub window: adw::ApplicationWindow,
    pub host: adw::EntryRow,
    pub port: adw::EntryRow,
    pub username: adw::EntryRow,
    pub password: adw::PasswordEntryRow,
    pub button: gtk::Button,
    pub status: gtk::Label,
    pub error: gtk::Label,
    pub http: adw::ActionRow,
    pub socks: adw::ActionRow,
    pub state: Rc<Cell<&'static str>>,
    settings: SharedSettings,
    commands: mpsc::Sender<Control>,
    pub settings_window: Rc<RefCell<Option<adw::PreferencesWindow>>>,
}
fn label(text: &str, class: &str) -> gtk::Label {
    let label = gtk::Label::builder().label(text).wrap(true).build();
    label.add_css_class(class);
    label
}
impl View {
    pub fn build(
        app: &adw::Application,
        settings: SharedSettings,
        commands: mpsc::Sender<Control>,
    ) -> Result<Rc<Self>> {
        let value = settings
            .lock()
            .map_err(|_| Error("unsafe_settings"))?
            .value
            .clone();
        let window = adw::ApplicationWindow::builder()
            .application(app)
            .title("SKVOZ")
            .default_width(560)
            .default_height(840)
            .build();
        let outer = gtk::Box::new(gtk::Orientation::Vertical, 0);
        let header = adw::HeaderBar::new();
        let settings_button = gtk::Button::builder()
            .icon_name("emblem-system-symbolic")
            .tooltip_text("Настройки прокси")
            .build();
        header.pack_end(&settings_button);
        outer.append(&header);
        let body = gtk::Box::builder()
            .orientation(gtk::Orientation::Vertical)
            .spacing(16)
            .margin_top(20)
            .margin_bottom(28)
            .margin_start(24)
            .margin_end(24)
            .build();
        let hero = gtk::Box::new(gtk::Orientation::Vertical, 8);
        let icon = gtk::Image::from_icon_name("network-transmit-receive-symbolic");
        icon.set_pixel_size(48);
        icon.add_css_class("accent");
        hero.append(&icon);
        hero.append(&label("Соединение SKVOZ", "title-1"));
        hero.append(&label("Локальный прокси для ваших приложений", "dim-label"));
        body.append(&hero);
        let login = adw::PreferencesGroup::builder().title("Сервер").build();
        let host = adw::EntryRow::builder()
            .title("IP-адрес или имя сервера")
            .text(&value.host)
            .build();
        let port = adw::EntryRow::builder()
            .title("Внешний порт NATS")
            .text(value.port.to_string())
            .build();
        let username = adw::EntryRow::builder()
            .title("Логин NATS")
            .text(&value.username)
            .build();
        let password = adw::PasswordEntryRow::builder()
            .title("Пароль NATS")
            .build();
        login.add(&host);
        login.add(&port);
        login.add(&username);
        login.add(&password);
        body.append(&login);
        let button = gtk::Button::builder()
            .label("Подключиться")
            .height_request(44)
            .build();
        button.add_css_class("suggested-action");
        button.add_css_class("pill");
        body.append(&button);
        let status = label("Отключено", "heading");
        body.append(&status);
        let error = label("", "error");
        error.set_visible(false);
        body.append(&error);
        let addresses = adw::PreferencesGroup::builder()
            .title("Адреса для приложений")
            .description("Укажите один из адресов в настройках браузера или другого приложения.")
            .build();
        let http = adw::ActionRow::builder()
            .title("HTTP / HTTPS CONNECT")
            .subtitle(format!("http://127.0.0.1:{}", value.http_port))
            .build();
        let socks = adw::ActionRow::builder()
            .title("SOCKS5 · DNS на сервере")
            .subtitle(format!("socks5h://127.0.0.1:{}", value.socks_port))
            .build();
        addresses.add(&http);
        addresses.add(&socks);
        body.append(&addresses);
        body.append(&label(
            "Пароль хранится только до отключения.\nИзменение системного прокси не требуется.",
            "dim-label",
        ));
        let clamp = adw::Clamp::builder()
            .maximum_size(480)
            .tightening_threshold(400)
            .child(&body)
            .build();
        let scroll = gtk::ScrolledWindow::builder()
            .vexpand(true)
            .hscrollbar_policy(gtk::PolicyType::Never)
            .child(&clamp)
            .build();
        outer.append(&scroll);
        window.set_content(Some(&outer));
        let view = Rc::new(Self {
            window,
            host,
            port,
            username,
            password,
            button,
            status,
            error,
            http,
            socks,
            state: Rc::new(Cell::new("disconnected")),
            settings,
            commands,
            settings_window: Rc::new(RefCell::new(None)),
        });
        let captured = Rc::downgrade(&view);
        settings_button.connect_clicked(move |_| {
            if let Some(captured) = captured.upgrade() {
                captured.show_settings();
            }
        });
        let captured = Rc::downgrade(&view);
        view.button.connect_clicked(move |_| {
            let Some(captured) = captured.upgrade() else {
                return;
            };
            if ["connecting", "connected", "reconnecting"].contains(&captured.state.get()) {
                captured.password.set_text("");
                let _ = captured.commands.try_send(Control::Disconnect);
            } else {
                let password = captured.password.text().to_string();
                captured.password.set_text("");
                match crate::settings::port(&captured.port.text(), false) {
                    Ok(port) => {
                        captured.apply(&Status {
                            state: "connecting",
                            error: None,
                            peer_id: None,
                            pid: None,
                            runtime: None,
                            connections: 0,
                            info: false,
                        });
                        if captured
                            .commands
                            .try_send(Control::Connect {
                                host: captured.host.text().to_string(),
                                port,
                                username: captured.username.text().to_string(),
                                password,
                            })
                            .is_err()
                        {
                            captured.show_error("core_unavailable");
                        }
                    }
                    Err(error) => captured.show_error(error.0),
                }
            }
        });
        Ok(view)
    }
    pub fn show_error(&self, code: &str) {
        self.error.set_label(message(code));
        self.error.set_visible(true);
    }
    pub fn apply(&self, status: &Status) {
        self.state.set(status.state);
        self.status.set_label(state_name(status.state));
        let active = ["connecting", "connected", "reconnecting"].contains(&status.state);
        self.button.set_label(if active {
            "Отключиться"
        } else {
            "Подключиться"
        });
        self.button.remove_css_class(if active {
            "suggested-action"
        } else {
            "destructive-action"
        });
        self.button.add_css_class(if active {
            "destructive-action"
        } else {
            "suggested-action"
        });
        self.host.set_sensitive(!active);
        self.port.set_sensitive(!active);
        self.username.set_sensitive(!active);
        self.password.set_sensitive(!active);
        self.error.set_visible(status.error.is_some());
        if let Some(error) = status.error {
            self.show_error(error);
        }
    }
    pub fn show_settings(self: &Rc<Self>) {
        if let Some(window) = self.settings_window.borrow().as_ref() {
            window.present();
            return;
        }
        let value = match self.settings.lock() {
            Ok(settings) => settings.value.clone(),
            Err(_) => return,
        };
        let dialog = adw::PreferencesWindow::builder()
            .transient_for(&self.window)
            .modal(true)
            .title("Настройки прокси")
            .default_width(520)
            .default_height(420)
            .build();
        let page = adw::PreferencesPage::new();
        let group = adw::PreferencesGroup::builder()
            .title("Локальные порты")
            .description("Только 127.0.0.1. Изменения применяются после следующего подключения.")
            .build();
        let http = adw::EntryRow::builder()
            .title("HTTP / HTTPS CONNECT")
            .text(value.http_port.to_string())
            .build();
        let socks = adw::EntryRow::builder()
            .title("SOCKS5 CONNECT")
            .text(value.socks_port.to_string())
            .build();
        group.add(&http);
        group.add(&socks);
        let settings_error = label("", "error");
        settings_error.set_visible(false);
        group.add(&settings_error);
        let save = gtk::Button::builder()
            .label("Сохранить")
            .margin_top(16)
            .build();
        save.add_css_class("suggested-action");
        group.add(&save);
        page.add(&group);
        let advanced=adw::PreferencesGroup::builder().title("Дополнительно").description("По умолчанию используются системные доверенные сертификаты. Для частного сервера укажите PEM-файл CA, полученный от администратора.").build();
        let ca = adw::EntryRow::builder()
            .title("Файл частного CA (необязательно)")
            .text(value.ca_file)
            .build();
        advanced.add(&ca);
        page.add(&advanced);
        dialog.add(&page);
        let captured = Rc::downgrade(self);
        let weak = dialog.downgrade();
        save.connect_clicked(move |_| {
            let Some(captured) = captured.upgrade() else {
                return;
            };
            let result = (|| {
                if ["connecting", "connected", "reconnecting"].contains(&captured.state.get()) {
                    return Err(Error("already_connected"));
                }
                let mut settings = captured
                    .settings
                    .lock()
                    .map_err(|_| Error("unsafe_settings"))?;
                let mut value = settings.value.clone();
                value.http_port = port(&http.text(), true)?;
                value.socks_port = port(&socks.text(), true)?;
                value.ca_file = ca.text().trim().to_owned();
                if value.ca_file.len() > 4096 {
                    return Err(Error("invalid_ca"));
                }
                settings.save(value.clone())?;
                captured
                    .http
                    .set_subtitle(&format!("http://127.0.0.1:{}", value.http_port));
                captured
                    .socks
                    .set_subtitle(&format!("socks5h://127.0.0.1:{}", value.socks_port));
                Ok(())
            })();
            match result {
                Ok(()) => {
                    if let Some(dialog) = weak.upgrade() {
                        dialog.close();
                    }
                }
                Err(error) => {
                    settings_error.set_label(message(error.0));
                    settings_error.set_visible(true);
                }
            }
        });
        let reference = Rc::downgrade(&self.settings_window);
        dialog.connect_close_request(move |_| {
            if let Some(reference) = reference.upgrade() {
                reference.borrow_mut().take();
            }
            glib::Propagation::Proceed
        });
        *self.settings_window.borrow_mut() = Some(dialog.clone());
        dialog.present();
    }
}
pub fn main() -> glib::ExitCode {
    let app = adw::Application::builder()
        .application_id("org.skvoz.Client")
        .flags(gio::ApplicationFlags::empty())
        .build();
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
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Client runtime initialization failed");
            runtime.block_on(run(engine, rx, status_tx));
        });
        let built = match View::build(app, settings, tx.clone()) {
            Ok(view) => view,
            Err(_) => return,
        };
        let app_copy = app.clone();
        built.window.connect_close_request(move |window| {
            window.set_sensitive(false);
            let _ = tx.try_send(Control::Quit);
            glib::Propagation::Stop
        });
        let ui = built.clone();
        glib::timeout_add_local(Duration::from_millis(100), move || {
            for _ in 0..8 {
                match status_rx.try_recv() {
                    Ok(status) => {
                        if status.state == "stopped" {
                            app_copy.quit();
                            return glib::ControlFlow::Break;
                        }
                        ui.apply(&status);
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
        built.window.present();
        *existing.borrow_mut() = Some(built);
    });
    let code = app.run();
    if let Some(view) = view.borrow_mut().take() {
        let dialog = view.settings_window.borrow_mut().take();
        let window = view.window.clone();
        drop(view);
        if let Some(dialog) = dialog {
            dialog.destroy();
        }
        window.destroy();
    }
    code
}
