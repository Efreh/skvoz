# SKVOZ для Ubuntu

Нативное Rust-приложение с GTK4/libadwaita управляет общим
`skvoz-network-runtime`: локальными интерфейсами
HTTP/HTTPS CONNECT и SOCKS5 CONNECT либо передачей IP-пакетов через Linux TUN.
Фоновая работа, индикатор панели, счётчики скорости
и ограниченный журнал назначений остаются в приложении. Deb для Ubuntu 24.04+
amd64 включает runtime и узкий системный `skvoz-network-helper`; Rust и Python
на пользовательской машине не требуются.

[Установка, подключение, режимы и проверки](../../docs/ubuntu-client.md)
описаны в каноническом руководстве. `src/app.rs` связывает жизненный цикл
приложения, backend и desktop; `src/ui.rs` строит окна и обрабатывает действия
пользователя. `src/ipc.rs` передаёт команды API1 и события по отдельному
управляющему сокету; полезные данные через этот сокет не проходят.
`tests/` содержит GTK-проверку, `spec/` — обычные RSpec процессные сценарии
с независимым Rust-приложением и реальным TLS NATS, `packaging/` — сборку deb,
desktop/icon files и обслуживание системных socket instances.

Для проверок управляющего слоя без GTK:

```sh
cargo test --locked -p skvoz-ubuntu-client --no-default-features --lib --features skvoz-network/linux-runtime
```

Стандартная сборка включает feature `desktop`. Headless-проверки не подтверждают
работу установленного GUI, системного helper, маршрутов или DNS.
