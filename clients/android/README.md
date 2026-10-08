# Клиент SKVOZ для Android

Kotlin/Compose-приложение с тем же Rust Core и сетевым runtime внутри APK.
Два режима: локальный HTTP/CONNECT/SOCKS5 TCP-прокси и L3 ВПН через Android
`VpnService`. Трафик обрабатывается в Rust; Kotlin управляет интерфейсом,
сохранённым профилем, service lifecycle и конфигурацией TUN.

[Настройка, сборка, ограничения и проверка](../../docs/android-client.md).

- `app/` — Compose, foreground services, Android Keystore/DataStore и platform tests.
- `native/` — узкий bounded JNI адаптер общего `network`/Core и JVM fixture.
- `tools/` — сборка двух native ABI и проверка ELF/APK alignment/signature.
- `spec/` — реальные Linux JVM/JNI + TLS/NATS HTTP/CONNECT/SOCKS consumers.
- `gradle/`, `gradlew` — checksum-pinned Gradle wrapper.

Android-only изменения проверяет собственный workflow `android-client.yml`.
Общие Rust sources/lock проверяют зависимые компоненты. Тесты Linux JVM не
заменяют проверку Android ВПН, Android Keystore и маршрутов выбранных приложений.
