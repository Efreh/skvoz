# SKVOZ для Ubuntu

<img src="../../assets/branding/skvoz-256.png" alt="Знак SKVOZ" width="96" height="96">

Нативное Rust-приложение с GTK4/libadwaita управляет общим
`skvoz-network-runtime`: локальными интерфейсами
HTTP/HTTPS CONNECT и SOCKS5 CONNECT либо передачей IP-пакетов через Linux TUN.
Фоновая работа, индикатор панели, счётчики скорости
и ограниченный журнал назначений остаются в приложении. Deb для Ubuntu 24.04+
amd64 включает runtime и узкий системный `skvoz-network-helper`; Rust и Python
на пользовательской машине не требуются.

Ubuntu6.0.0 использует runtime0.6.0, network6, enrollment4 и API1.
Полноценный сервер SKVOZ7 выдаёт устройство автоматически; общий Core4.1.0
соединяет клиент с peer0 через native NATS.

[Установка, подключение, режимы и проверки](../../docs/ubuntu-client.md)
описаны в каноническом руководстве. `src/app.rs` связывает жизненный цикл
приложения, backend и desktop; `src/ui.rs` строит окна и обрабатывает действия
пользователя. `src/ipc.rs` передаёт команды API1 и события по отдельному
управляющему сокету; полезные данные через этот сокет не проходят.
`tests/` содержит GTK-проверку, `spec/` — обычные RSpec процессные сценарии
с независимым Rust-приложением и реальным TLS NATS, `packaging/` — сборку deb,
desktop files и обслуживание системных socket instances. Цветные иконки в
`assets/icons/` устанавливаются в `hicolor`; общие исходники и правила —
[в каталоге знака проекта](../../assets/branding/README.md).

Для проверок управляющего слоя без GTK:

```sh
cargo test --locked -p skvoz-ubuntu-client --no-default-features --lib --features skvoz-network/linux-runtime
```

Стандартная сборка включает feature `desktop`. Headless-проверки не подтверждают
работу установленного GUI, системного helper, маршрутов или DNS.
