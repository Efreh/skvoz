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
pub fn state_name(state: &str) -> &'static str {
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
    pub speed: gtk::Label,
    pub status_icon: gtk::Image,
    pub tray_notice: gtk::Label,
    pub telemetry: Arc<crate::telemetry::Telemetry>,
    pub log_window: Rc<RefCell<Option<adw::Window>>>,
    log_text: gtk::TextBuffer,
    pub preview: gtk::TextView,
    preview_revision: Cell<u64>,
    log_revision: Cell<u64>,
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
fn copy_address(row: &adw::ActionRow) {
    let button = gtk::Button::builder()
        .icon_name("edit-copy-symbolic")
        .tooltip_text("Скопировать адрес")
        .valign(gtk::Align::Center)
        .build();
    button.add_css_class("flat");
    let weak = row.downgrade();
    button.connect_clicked(move |button| {
        if let Some(row) = weak.upgrade()
            && let Some(address) = row.subtitle()
        {
            button.clipboard().set_text(&address);
            button.set_icon_name("emblem-ok-symbolic");
            button.set_tooltip_text(Some("Адрес скопирован"));
            let weak = button.downgrade();
            glib::timeout_add_local_once(Duration::from_millis(1500), move || {
                if let Some(button) = weak.upgrade() {
                    button.set_icon_name("edit-copy-symbolic");
                    button.set_tooltip_text(Some("Скопировать адрес"));
                }
            });
        }
    });
    row.add_suffix(&button);
}
impl View {
    pub fn build(
        app: &adw::Application,
        settings: SharedSettings,
        commands: mpsc::Sender<Control>,
    ) -> Result<Rc<Self>> {
        let enabled = settings
            .lock()
            .map_err(|_| Error("unsafe_settings"))?
            .value
            .request_log;
        Self::build_with_telemetry(
            app,
            settings,
            commands,
            crate::telemetry::Telemetry::new(enabled),
        )
    }
    pub fn build_with_telemetry(
        app: &adw::Application,
        settings: SharedSettings,
        commands: mpsc::Sender<Control>,
        telemetry: Arc<crate::telemetry::Telemetry>,
    ) -> Result<Rc<Self>> {
        let value = settings
            .lock()
            .map_err(|_| Error("unsafe_settings"))?
            .value
            .clone();
        let window = adw::ApplicationWindow::builder()
            .application(app)
            .title("Соединение SKVOZ")
            .default_width(560)
            .default_height(840)
            .build();
        let outer = gtk::Box::new(gtk::Orientation::Vertical, 0);
        let header = adw::HeaderBar::new();
        let settings_button = gtk::Button::builder()
            .icon_name("emblem-system-symbolic")
            .tooltip_text("Настройки соединения")
            .build();
        header.pack_end(&settings_button);
        let menu = gio::Menu::new();
        for (title, action) in [
            ("Показать SKVOZ", "app.show"),
            ("Журнал соединений", "app.log"),
            ("Настройки соединения", "app.settings"),
            ("Завершить SKVOZ", "app.quit"),
        ] {
            menu.append(Some(title), Some(action));
        }
        let menu_button = gtk::MenuButton::builder()
            .icon_name("open-menu-symbolic")
            .menu_model(&menu)
            .build();
        header.pack_end(&menu_button);
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
            .text(value.saved_password(&value.host, value.port, &value.username))
            .build();
        for row in [&host, &port, &username] {
            let host = host.downgrade();
            let port = port.downgrade();
            let username = username.downgrade();
            let password = password.downgrade();
            let settings = Arc::downgrade(&settings);
            row.connect_changed(move |_| {
                let (Some(host), Some(port), Some(username), Some(password)) = (
                    host.upgrade(),
                    port.upgrade(),
                    username.upgrade(),
                    password.upgrade(),
                ) else {
                    return;
                };
                let Some(settings) = settings.upgrade() else {
                    return;
                };
                let saved = settings
                    .lock()
                    .ok()
                    .and_then(|settings| {
                        crate::settings::port(&port.text(), false).ok().map(|port| {
                            settings
                                .value
                                .saved_password(&host.text(), port, &username.text())
                        })
                    })
                    .unwrap_or_default();
                password.set_text(&saved);
            });
        }
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
        let status_box = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        status_box.set_halign(gtk::Align::Center);
        let status_icon = gtk::Image::from_icon_name("network-offline-symbolic");
        status_box.append(&status_icon);
        status_box.append(&status);
        body.append(&status_box);
        let speed = label("↓ 0 Б/с   ↑ 0 Б/с", "monospace");
        speed.set_tooltip_text(Some("Получено и отправлено через SKVOZ за секунду"));
        body.append(&speed);
        let activity = adw::PreferencesGroup::builder()
            .title("Соединения")
            .description("Последние события SKVOZ")
            .build();
        let log_button = gtk::Button::with_label("Подробнее");
        activity.set_header_suffix(Some(&log_button));
        let preview = gtk::TextView::builder()
            .editable(false)
            .cursor_visible(false)
            .monospace(true)
            .wrap_mode(gtk::WrapMode::WordChar)
            .left_margin(10)
            .right_margin(10)
            .top_margin(10)
            .bottom_margin(10)
            .build();
        preview.buffer().set_text(if value.request_log {
            "Соединений пока нет."
        } else {
            "Журнал выключен в настройках."
        });
        let preview_scroll = gtk::ScrolledWindow::builder()
            .height_request(150)
            .hscrollbar_policy(gtk::PolicyType::Never)
            .child(&preview)
            .build();
        preview_scroll.add_css_class("card");
        activity.add(&preview_scroll);
        body.append(&activity);
        let tray_notice = label(
            "Панель не поддерживает индикатор SKVOZ. После скрытия окно можно открыть из меню приложений.",
            "dim-label",
        );
        body.append(&tray_notice);
        let error = label("", "error");
        error.set_visible(false);
        body.append(&error);
        let addresses = adw::PreferencesGroup::builder()
            .title("Локальные адреса")
            .description("Укажите один из адресов в настройках браузера или другого приложения.")
            .build();
        let http = adw::ActionRow::builder()
            .title("HTTP / HTTPS")
            .subtitle(format!("http://127.0.0.1:{}", value.http_port))
            .build();
        let socks = adw::ActionRow::builder()
            .title("SOCKS5 · DNS на сервере")
            .subtitle(format!("socks5h://127.0.0.1:{}", value.socks_port))
            .build();
        copy_address(&http);
        copy_address(&socks);
        addresses.add(&http);
        addresses.add(&socks);
        body.append(&addresses);
        body.append(&label(
            "Пароль сохраняется на этом устройстве.\nЗакрытие окна оставляет SKVOZ работать в фоне.",
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
            speed,
            status_icon,
            tray_notice,
            telemetry,
            log_window: Rc::new(RefCell::new(None)),
            log_text: gtk::TextBuffer::new(None::<&gtk::TextTagTable>),
            preview,
            preview_revision: Cell::new(u64::MAX),
            log_revision: Cell::new(u64::MAX),
            error,
            http,
            socks,
            state: Rc::new(Cell::new("disconnected")),
            settings,
            commands,
            settings_window: Rc::new(RefCell::new(None)),
        });
        for (name, id) in [("show", 1), ("log", 5), ("settings", 6), ("quit", 7)] {
            let action = gio::SimpleAction::new(name, None);
            let weak = Rc::downgrade(&view);
            action.connect_activate(move |_, _| {
                if let Some(view) = weak.upgrade() {
                    match id {
                        1 => view.window.present(),
                        5 => view.show_log(),
                        6 => view.show_settings(),
                        _ => view.quit(),
                    }
                }
            });
            app.add_action(&action);
        }
        let weak = Rc::downgrade(&view);
        log_button.connect_clicked(move |_| {
            if let Some(view) = weak.upgrade() {
                view.show_log();
            }
        });
        let weak = Rc::downgrade(&view);
        view.window.connect_close_request(move |window| {
            if let Some(view) = weak.upgrade() {
                if let Some(settings) = view.settings_window.borrow().as_ref() {
                    settings.set_visible(false);
                }
                if let Some(log) = view.log_window.borrow().as_ref() {
                    log.set_visible(false);
                }
            }
            window.set_visible(false);
            glib::Propagation::Stop
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
                let _ = captured.commands.try_send(Control::Disconnect);
            } else {
                let password = captured.password.text().to_string();
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
                            uploaded: 0,
                            downloaded: 0,
                            requests: Vec::new(),
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
    pub fn active(&self) -> bool {
        ["connecting", "connected", "reconnecting"].contains(&self.state.get())
    }
    pub fn settings_value(&self) -> Option<crate::settings::Preferences> {
        self.settings.lock().ok().map(|s| s.value.clone())
    }
    pub fn quit(&self) {
        if self.commands.try_send(Control::Quit).is_err() {
            self.show_error("core_unavailable");
        }
    }
    pub fn show_log(self: &Rc<Self>) {
        if let Some(window) = self.log_window.borrow().as_ref() {
            window.present();
            return;
        }
        let window = adw::Window::builder()
            .title("Журнал соединений")
            .transient_for(&self.window)
            .default_width(820)
            .default_height(480)
            .build();
        let outer = gtk::Box::new(gtk::Orientation::Vertical, 8);
        let header = adw::HeaderBar::new();
        let clear = gtk::Button::with_label("Очистить");
        header.pack_end(&clear);
        outer.append(&header);
        let hint = label(
            "Последние 500 событий: протокол, назначение, результат и байты. Содержимое соединений не записывается.",
            "dim-label",
        );
        outer.append(&hint);
        let text = gtk::TextView::builder()
            .buffer(&self.log_text)
            .editable(false)
            .cursor_visible(false)
            .monospace(true)
            .wrap_mode(gtk::WrapMode::WordChar)
            .left_margin(12)
            .right_margin(12)
            .top_margin(12)
            .build();
        let scroll = gtk::ScrolledWindow::builder()
            .vexpand(true)
            .child(&text)
            .build();
        outer.append(&scroll);
        window.set_content(Some(&outer));
        let telemetry = self.telemetry.clone();
        clear.connect_clicked(move |_| telemetry.clear());
        let reference = Rc::downgrade(&self.log_window);
        window.connect_close_request(move |_| {
            if let Some(reference) = reference.upgrade() {
                reference.borrow_mut().take();
            }
            glib::Propagation::Proceed
        });
        *self.log_window.borrow_mut() = Some(window.clone());
        self.log_revision.set(u64::MAX);
        window.present();
        self.refresh_log();
    }
    pub fn refresh_log(&self) {
        let revision = self.telemetry.revision();
        let preview = self.window.is_visible() && self.preview_revision.get() != revision;
        let detail = self
            .log_window
            .borrow()
            .as_ref()
            .is_some_and(|w| w.is_visible())
            && self.log_revision.get() != revision;
        if !preview && !detail {
            return;
        }
        let entries = self.telemetry.history();
        let render = |limit: usize| {
            let mut text = String::new();
            for request in entries.iter().rev().take(limit) {
                let time = glib::DateTime::from_unix_local(request.time as i64)
                    .ok()
                    .and_then(|t| t.format("%H:%M:%S").ok())
                    .map(|t| t.to_string())
                    .unwrap_or_default();
                let result = match request.result {
                    "opening" => "Открытие",
                    "active" => "Открыто",
                    "finished" => "Завершено",
                    "cancelled" => "Прервано",
                    _ => "Ошибка",
                };
                let host = if request.host.contains(':') {
                    format!("[{}]", request.host)
                } else {
                    request.host.clone()
                };
                text.push_str(&format!(
                    "{time}  #{}  {}  {host}:{}  {result}  ↑ {} Б  ↓ {} Б\n",
                    request.id,
                    request.protocol,
                    request.port,
                    request.uploaded,
                    request.downloaded
                ));
            }
            let skipped = self
                .telemetry
                .skipped
                .load(std::sync::atomic::Ordering::Relaxed);
            if skipped > 0 {
                text.push_str(&format!(
                    "Пропущено событий при занятом журнале: {skipped}\n"
                ));
            }
            if text.is_empty() {
                text = if self.settings_value().is_some_and(|s| s.request_log) {
                    "Соединений пока нет."
                } else {
                    "Журнал выключен в настройках."
                }
                .into();
            }
            text
        };
        if preview {
            self.preview.buffer().set_text(&render(10));
            self.preview_revision.set(revision);
        }
        if detail {
            self.log_text
                .set_text(&render(crate::telemetry::HISTORY_LIMIT));
            self.log_revision.set(revision);
        }
    }
    pub fn show_error(&self, code: &str) {
        self.error.set_label(message(code));
        self.error.set_visible(true);
    }
    pub fn apply(&self, status: &Status) {
        self.state.set(status.state);
        self.status.set_label(state_name(status.state));
        self.status_icon.set_icon_name(Some(match status.state {
            "connected" => "network-transmit-receive-symbolic",
            "connecting" | "reconnecting" => "network-idle-symbolic",
            "error" => "network-error-symbolic",
            _ => "network-offline-symbolic",
        }));
        for class in ["success", "warning", "error"] {
            self.status_icon.remove_css_class(class);
        }
        if status.state == "connected" {
            self.status_icon.add_css_class("success");
        } else if status.state == "error" {
            self.status_icon.add_css_class("error");
        } else if status.state == "connecting" || status.state == "reconnecting" {
            self.status_icon.add_css_class("warning");
        }
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
            .title("Настройки соединения")
            .default_width(520)
            .default_height(420)
            .build();
        let page = adw::PreferencesPage::new();
        let group = adw::PreferencesGroup::builder()
            .title("Локальные порты")
            .description("Только 127.0.0.1. Изменения применяются после следующего подключения.")
            .build();
        let http = adw::EntryRow::builder()
            .title("HTTP / HTTPS")
            .text(value.http_port.to_string())
            .build();
        let socks = adw::EntryRow::builder()
            .title("SOCKS5")
            .text(value.socks_port.to_string())
            .build();
        group.add(&http);
        group.add(&socks);
        let settings_error = label("", "error");
        settings_error.set_visible(false);

        let save = gtk::Button::builder()
            .label("Сохранить")
            .margin_top(16)
            .build();
        save.add_css_class("suggested-action");

        page.add(&group);
        let advanced=adw::PreferencesGroup::builder().title("Дополнительно").description("По умолчанию используются системные доверенные сертификаты. Для частного сервера укажите PEM-файл CA, полученный от администратора.").build();
        let ca = adw::EntryRow::builder()
            .title("Файл частного CA (необязательно)")
            .text(value.ca_file)
            .build();
        advanced.add(&ca);
        page.add(&advanced);
        let background = adw::PreferencesGroup::builder()
            .title("Фоновая работа")
            .description(
                "Закрытие окна скрывает SKVOZ. Полная остановка — через «Завершить SKVOZ».",
            )
            .build();
        let autostart = adw::SwitchRow::builder()
            .title("Запускать при входе в систему")
            .active(value.autostart)
            .build();
        let auto_connect = adw::SwitchRow::builder()
            .title("Подключаться при запуске")
            .subtitle("Использовать сохранённое соединение")
            .active(value.auto_connect)
            .build();
        let tray_speed = adw::SwitchRow::builder()
            .title("Скорость рядом с индикатором")
            .subtitle("Если панель рабочего стола поддерживает текст")
            .active(value.tray_speed)
            .build();
        let request_log = adw::SwitchRow::builder()
            .title("Журнал соединений")
            .subtitle("Только в памяти; выключение очищает журнал")
            .active(value.request_log)
            .build();
        for row in [&autostart, &auto_connect, &tray_speed, &request_log] {
            background.add(row);
        }
        background.add(&settings_error);
        background.add(&save);
        page.add(&background);
        dialog.add(&page);
        let captured = Rc::downgrade(self);
        let weak = dialog.downgrade();
        save.connect_clicked(move |_| {
            let Some(captured) = captured.upgrade() else {
                return;
            };
            let result = (|| {
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
                if captured.active()
                    && (value.http_port != settings.value.http_port
                        || value.socks_port != settings.value.socks_port
                        || value.ca_file != settings.value.ca_file)
                {
                    return Err(Error("already_connected"));
                }
                value.autostart = autostart.is_active();
                value.auto_connect = auto_connect.is_active();
                value.tray_speed = tray_speed.is_active();
                value.request_log = request_log.is_active();
                if value.autostart != settings.value.autostart {
                    let base = settings
                        .directory
                        .parent()
                        .ok_or(Error("unsafe_settings"))?;
                    crate::desktop::autostart(base, value.autostart)?;
                }
                let old_autostart = settings.value.autostart;
                if let Err(error) = settings.save(value.clone()) {
                    if value.autostart != old_autostart {
                        let _ = crate::desktop::autostart(
                            settings
                                .directory
                                .parent()
                                .ok_or(Error("unsafe_settings"))?,
                            old_autostart,
                        );
                    }
                    return Err(error);
                }
                captured.telemetry.enable(value.request_log);
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
            runtime.block_on(run(engine, rx, status_tx));
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
            view.speed.set_label(&format!(
                "↓ {}   ↑ {}",
                crate::telemetry::rate(down.saturating_sub(previous.1), seconds),
                crate::telemetry::rate(up.saturating_sub(previous.0), seconds)
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
