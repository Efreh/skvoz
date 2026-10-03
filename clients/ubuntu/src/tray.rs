//! StatusNotifierItem/DBusMenu on the GTK main context, without a GTK3 library.
use crate::ui::{View, state_name};
use glib::variant::ToVariant;
use gtk::{gio, glib, prelude::*};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    rc::{Rc, Weak},
};
const ITEM: &str = "/StatusNotifierItem";
const MENU: &str = "/Menu";
const WATCHER: &str = "org.kde.StatusNotifierWatcher";
type Properties = BTreeMap<String, glib::Variant>;
type Layout = (i32, Properties, Vec<glib::Variant>);
pub struct Tray {
    bus: gio::DBusConnection,
    registrations: Vec<gio::RegistrationId>,
    watcher: Option<Box<dyn FnOnce()>>,
    pub available: Rc<Cell<bool>>,
    revision: Rc<Cell<u32>>,
    label: Rc<RefCell<String>>,
    view: Weak<View>,
    previous: RefCell<(String, String, String)>,
}
fn properties(view: &View, id: i32) -> Option<Properties> {
    let active = view.active();
    let (text, enabled) = match id {
        0 => ("".to_owned(), true),
        1 => ("Показать SKVOZ".into(), true),
        2 => ("Подключиться".into(), !active),
        3 => ("Отключиться".into(), active),
        4 => (
            format!("{} · {}", state_name(view.state.get()), view.speed.text()),
            false,
        ),
        5 => ("Журнал соединений".into(), true),
        6 => ("Настройки соединения".into(), true),
        7 => ("Завершить SKVOZ".into(), true),
        _ => return None,
    };
    let mut properties = Properties::from([
        ("label".into(), text.to_variant()),
        ("enabled".into(), enabled.to_variant()),
        ("visible".into(), true.to_variant()),
    ]);
    if id == 0 {
        properties.insert("children-display".into(), "submenu".to_variant());
    }
    Some(properties)
}
fn filtered(mut properties: Properties, names: &[String]) -> Properties {
    if !names.is_empty() {
        properties.retain(|name, _| names.contains(name));
    }
    properties
}
fn layout(view: &View, id: i32, depth: i32, names: &[String]) -> Option<Layout> {
    let properties = filtered(properties(view, id)?, names);
    let children = if id == 0 && depth != 0 {
        (1..=7)
            .filter_map(|child| layout(view, child, 0, names).map(|value| value.to_variant()))
            .collect()
    } else {
        Vec::new()
    };
    Some((id, properties, children))
}
fn action(view: &Rc<View>, id: i32) -> bool {
    match id {
        1 => view.window.present(),
        2 if !view.active() => view.button.emit_clicked(),
        3 if view.active() => view.button.emit_clicked(),
        5 => view.show_log(),
        6 => view.show_settings(),
        7 => view.quit(),
        _ => return false,
    }
    true
}
impl Tray {
    pub fn new(bus: gio::DBusConnection, view: &Rc<View>) -> Result<Self, glib::Error> {
        let node = gio::DBusNodeInfo::for_xml(include_str!("tray.xml"))?;
        let weak = Rc::downgrade(view);
        let property_view = weak.clone();
        let label = Rc::new(RefCell::new(String::new()));
        let property_label = label.clone();
        let item = bus
            .register_object(
                ITEM,
                &node.lookup_interface("org.kde.StatusNotifierItem").unwrap(),
            )
            .method_call(move |_, _, _, _, method, _, invocation| {
                if let Some(view) = weak.upgrade()
                    && ["Activate", "SecondaryActivate", "ContextMenu"].contains(&method)
                {
                    view.window.present();
                }
                invocation.return_value(Some(&().to_variant()));
            })
            .property(move |_, _, _, _, property| {
                let state = property_view
                    .upgrade()
                    .map(|v| v.state.get())
                    .unwrap_or("disconnected");
                match property {
                    "Category" => "Communications".to_variant(),
                    "Id" => "org.skvoz.Client".to_variant(),
                    "Title" => "Соединение SKVOZ".to_variant(),
                    "Status" => if state == "error" {
                        "NeedsAttention"
                    } else {
                        "Active"
                    }
                    .to_variant(),
                    "WindowId" => 0i32.to_variant(),
                    "Menu" => glib::variant::ObjectPath::try_from(MENU)
                        .unwrap()
                        .to_variant(),
                    "ItemIsMenu" => true.to_variant(),
                    "IconName" | "AttentionIconName" => icon(state).to_variant(),
                    "IconPixmap" | "AttentionIconPixmap" | "OverlayIconPixmap" => {
                        Vec::<(i32, i32, Vec<u8>)>::new().to_variant()
                    }
                    "ToolTip" => (
                        icon(state),
                        Vec::<(i32, i32, Vec<u8>)>::new(),
                        "SKVOZ",
                        format!(
                            "{} · {}",
                            state_name(state),
                            property_view
                                .upgrade()
                                .map(|view| view.speed.text().to_string())
                                .unwrap_or_default()
                        ),
                    )
                        .to_variant(),
                    "XAyatanaLabel" => property_label.borrow().to_variant(),
                    "XAyatanaLabelGuide" => "↓ 999.9 МиБ/с ↑ 999.9 МиБ/с".to_variant(),
                    _ => "".to_variant(),
                }
            })
            .build()?;
        let revision = Rc::new(Cell::new(1));
        let menu_revision = revision.clone();
        let weak = Rc::downgrade(view);
        let menu = bus
            .register_object(
                MENU,
                &node.lookup_interface("com.canonical.dbusmenu").unwrap(),
            )
            .property(|_, _, _, _, property| match property {
                "Version" => 3u32.to_variant(),
                "TextDirection" => "ltr".to_variant(),
                "Status" => "normal".to_variant(),
                _ => Vec::<String>::new().to_variant(),
            })
            .method_call(move |_, _, _, _, method, parameters, invocation| {
                let Some(view) = weak.upgrade() else {
                    invocation.return_dbus_error(
                        "org.freedesktop.DBus.Error.Failed",
                        "Application stopped",
                    );
                    return;
                };
                let result = match method {
                    "GetLayout" => parameters.get::<(i32, i32, Vec<String>)>().and_then(
                        |(id, depth, names)| {
                            layout(&view, id, depth, &names)
                                .map(|value| (menu_revision.get(), value).to_variant())
                        },
                    ),
                    "GetGroupProperties" => {
                        parameters
                            .get::<(Vec<i32>, Vec<String>)>()
                            .map(|(ids, names)| {
                                let ids = if ids.is_empty() {
                                    (0..=7).collect()
                                } else {
                                    ids
                                };
                                (ids.into_iter()
                                    .filter_map(|id| {
                                        properties(&view, id).map(|p| (id, filtered(p, &names)))
                                    })
                                    .collect::<Vec<_>>(),)
                                    .to_variant()
                            })
                    }
                    "GetProperty" => parameters.get::<(i32, String)>().and_then(|(id, name)| {
                        properties(&view, id)?
                            .remove(&name)
                            .map(|value| (value,).to_variant())
                    }),
                    "Event" => parameters.get::<(i32, String, glib::Variant, u32)>().map(
                        |(id, event, _, _)| {
                            if event == "clicked" {
                                action(&view, id);
                            }
                            ().to_variant()
                        },
                    ),
                    "EventGroup" => parameters
                        .get::<(Vec<(i32, String, glib::Variant, u32)>,)>()
                        .map(|(events,)| {
                            let errors = events
                                .into_iter()
                                .filter_map(|(id, event, _, _)| {
                                    if event == "clicked" && !action(&view, id) {
                                        Some(id)
                                    } else {
                                        None
                                    }
                                })
                                .collect::<Vec<_>>();
                            (errors,).to_variant()
                        }),
                    "AboutToShow" => Some((false,).to_variant()),
                    "AboutToShowGroup" => Some((Vec::<i32>::new(), Vec::<i32>::new()).to_variant()),
                    _ => None,
                };
                if let Some(result) = result {
                    invocation.return_value(Some(&result));
                } else {
                    invocation.return_dbus_error(
                        "org.freedesktop.DBus.Error.InvalidArgs",
                        "Unknown menu item or method",
                    );
                }
            })
            .build();
        let menu = match menu {
            Ok(menu) => menu,
            Err(error) => {
                let _ = bus.unregister_object(item);
                return Err(error);
            }
        };
        let available = Rc::new(Cell::new(false));
        let ready = available.clone();
        let absent = available.clone();
        let weak = Rc::downgrade(view);
        let absent_view = weak.clone();
        let watcher = gio::bus_watch_name_on_connection(
            &bus,
            WATCHER,
            gio::BusNameWatcherFlags::NONE,
            move |bus, _, _| {
                let ready = ready.clone();
                let view = weak.clone();
                bus.call(
                    Some(WATCHER),
                    "/StatusNotifierWatcher",
                    WATCHER,
                    "RegisterStatusNotifierItem",
                    Some(&(ITEM,).to_variant()),
                    None,
                    gio::DBusCallFlags::NONE,
                    2000,
                    None::<&gio::Cancellable>,
                    move |result| {
                        ready.set(result.is_ok());
                        if let Some(view) = view.upgrade() {
                            view.tray_notice.set_visible(result.is_err());
                        }
                    },
                );
            },
            move |_, _| {
                absent.set(false);
                if let Some(view) = absent_view.upgrade() {
                    view.tray_notice.set_visible(true);
                    if !view.window.is_visible() {
                        view.window.present();
                    }
                }
            },
        );
        Ok(Self {
            bus,
            registrations: vec![item, menu],
            watcher: Some(Box::new(move || gio::bus_unwatch_name(watcher))),
            available,
            revision,
            label,
            view: Rc::downgrade(view),
            previous: RefCell::new((String::new(), String::new(), String::new())),
        })
    }
    pub fn update(&self) {
        let Some(view) = self.view.upgrade() else {
            return;
        };
        let label = if view.settings_value().is_some_and(|s| s.tray_speed) {
            view.speed.text().to_string()
        } else {
            String::new()
        };
        let state = view.state.get();
        let speed = view.speed.text().to_string();
        let previous = self.previous.borrow().clone();
        if previous == (state.to_owned(), speed.clone(), label.clone()) {
            return;
        }
        *self.previous.borrow_mut() = (state.to_owned(), speed.clone(), label.clone());
        *self.label.borrow_mut() = label.clone();
        let emit = |path, interface, signal, parameters: glib::Variant| {
            let _ = self
                .bus
                .emit_signal(None, path, interface, signal, Some(&parameters));
        };
        if previous.0 != state {
            self.revision.set(self.revision.get().wrapping_add(1));
            emit(
                MENU,
                "com.canonical.dbusmenu",
                "LayoutUpdated",
                (self.revision.get(), 0i32).to_variant(),
            );
            emit(
                ITEM,
                "org.kde.StatusNotifierItem",
                "NewIcon",
                ().to_variant(),
            );
            emit(
                ITEM,
                "org.kde.StatusNotifierItem",
                "NewStatus",
                (if state == "error" {
                    "NeedsAttention"
                } else {
                    "Active"
                },)
                    .to_variant(),
            );
        }
        if previous.0 != state || previous.1 != speed {
            let properties = Properties::from([(
                "label".into(),
                format!("{} · {speed}", state_name(state)).to_variant(),
            )]);
            emit(
                MENU,
                "com.canonical.dbusmenu",
                "ItemsPropertiesUpdated",
                (vec![(4i32, properties)], Vec::<(i32, Vec<String>)>::new()).to_variant(),
            );
            emit(
                ITEM,
                "org.kde.StatusNotifierItem",
                "NewToolTip",
                ().to_variant(),
            );
        }
        if previous.2 != label {
            emit(
                ITEM,
                "org.kde.StatusNotifierItem",
                "XAyatanaNewLabel",
                (label, "↓ 999.9 МиБ/с ↑ 999.9 МиБ/с").to_variant(),
            );
        }
    }
}
fn icon(state: &str) -> &'static str {
    match state {
        "connected" => "network-transmit-receive-symbolic",
        "connecting" | "reconnecting" => "network-idle-symbolic",
        "error" => "network-error-symbolic",
        _ => "network-offline-symbolic",
    }
}
impl Drop for Tray {
    fn drop(&mut self) {
        if let Some(watcher) = self.watcher.take() {
            watcher();
        }
        for id in self.registrations.drain(..) {
            let _ = self.bus.unregister_object(id);
        }
    }
}
