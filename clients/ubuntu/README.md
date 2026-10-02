# SKVOZ для Ubuntu

Нативное Rust-приложение с GTK4/libadwaita, локальными HTTP/HTTPS CONNECT и
SOCKS5 CONNECT прокси. Deb для Ubuntu 24.04+ amd64 включает скомпилированный
универсальный `skvoz-core-daemon`; Rust и Python на пользовательской машине
не требуются.

[Установка, подключение, порты, протокол выделения устройства и проверки](../../docs/ubuntu-client.md)
описаны в каноническом руководстве. `src/` содержит приложение и адаптеры IPC,
`tests/` — нативную GTK-проверку, `spec/` — обычные RSpec процессные сценарии
с независимым Rust-приложением и реальным TLS NATS, `packaging/` — простой
сборщик deb и desktop/icon files.
