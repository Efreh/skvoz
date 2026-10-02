# Клиенты SKVOZ

[Ubuntu](ubuntu/README.md) — нативное приложение Rust с GTK4/libadwaita,
HTTP/HTTPS CONNECT и SOCKS5 CONNECT, комплектным Core daemon и deb-поставкой.
[Руководство клиента](../docs/ubuntu-client.md).

[Python](python/README.md) и [Ruby](ruby/README.md) остаются самостоятельными
примерами IPC на стандартной библиотеке, не зависимостями Ubuntu-приложения.
Клиенты используют ту же универсальную Rust-библиотеку Core внутри
[демона](../docs/daemon-ipc.md).
