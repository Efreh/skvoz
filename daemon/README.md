# Демон Core SKVOZ

`skvoz-core-daemon` — исполняемый файл Linux с той же универсальной
[библиотекой Core](../core/README.md) и `NatsRuntime`. Приложения других языков
используют единый локальный [IPC v1](../docs/daemon-ipc.md) через закрытый Unix-сокет.
Каталог содержит управление процессом, профилем, сокетом и владельцами IPC.

Из корня репозитория:

```sh
cargo build --release -p skvoz-daemon --locked
./target/release/skvoz-core-daemon --help
python3 testbench/run.py daemon
```

Последняя команда проверяет независимые демоны сборки release и процессы
Python/Ruby через временный NATS с TLS. Нужны Linux, Rust 1.92+, Python 3.9+,
Ruby 3.4+, Docker и OpenSSL. `--offline` использует заранее скачанные зависимости
и образ. [Подготовка стенда](../docs/getting-started.md).

Для своего брокера подготовьте закрытый профиль по
[инструкции](../docs/daemon-ipc.md#сборка-запуск-и-профиль). `SKVOZ_PROFILE`
должен содержать абсолютный путь к нему:

```sh
./target/release/skvoz-core-daemon --check-config "$SKVOZ_PROFILE"
./target/release/skvoz-core-daemon --config "$SKVOZ_PROFILE"
```

Демон можно запускать как внешнюю службу или управляемый дочерний процесс.
Конфигурация, права файлов, запуск и остановка, бюджеты и бинарный контракт
описаны в [руководстве IPC](../docs/daemon-ipc.md). Демон передаёт непрозрачные
метаданные и байты; учётные данные, PeerId, сертификаты и политика назначения
принадлежат приложению. Прерванные потоки не возобновляются.

Входящие открытия получает один эксклюзивный обработчик; остальные IPC-сессии
владеют только своими исходящими потоками.
[Векторы](tests/fixtures/README.md), [пример Python](../clients/python/README.md)
и [пример Ruby](../clients/ruby/README.md) позволяют реализовать новый адаптер.
