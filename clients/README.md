# Клиенты SKVOZ

[Ubuntu](ubuntu/README.md) — нативное приложение Rust с GTK4/libadwaita,
HTTP/HTTPS CONNECT, SOCKS5 CONNECT и L3 ВПН, комплектным общим network runtime и deb-поставкой.
[Руководство клиента](../docs/ubuntu-client.md).

[Android](android/README.md) — Kotlin/Compose/Material3 со встроенным тем же Rust
Core/network, loopback Прокси и Android VpnService. [Руководство Android](../docs/android-client.md)
описывает APK, настройки и открытые проверки на устройстве.

[Python](python/README.md) и [Ruby](ruby/README.md) остаются самостоятельными
примерами IPC на стандартной библиотеке, не зависимостями Ubuntu-приложения.
Клиенты используют ту же универсальную Rust-библиотеку Core внутри
[демона](../docs/daemon-ipc.md).
