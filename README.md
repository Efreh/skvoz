# SKVOZ

SKVOZ — модульный проект для двунаправленных байтовых потоков через NATS.
Репозиторий объединяет переносимое ядро, будущие клиенты и вспомогательные
компоненты. Сейчас реализованы универсальная Rust-библиотека Core с multi-peer менеджером,
опциональным NATS runtime, standalone Linux daemon с единым IPC и минимальный TCP relay.

| Компонент | Назначение |
| --- | --- |
| [core/](core/README.md) | Stream/Manager без I/O, codec, статический NatsNode и dynamic NatsRuntime; общий контракт для всех коннекторов. |
| [daemon/](daemon/README.md) | Standalone host той же Core library: private Unix socket и language-independent IPC v1. |
| [testbench/](testbench/README.md) | Реальные NATS/TCP сценарии, много клиентов, измерение нагрузки. |
| [connectors/tcp/](connectors/tcp/README.md) | Экспериментальный relay одного TCP-сокета через встроенный Core. |
| [clients/](clients/README.md) | Python/Ruby standard-library IPC examples; законченных клиентских приложений пока нет. |

Rust-компоненты используют общий Cargo workspace и Cargo.lock. Каждый подпроект
хранит свои исходники, тесты и инструменты в собственном каталоге.

Для стенда нужны Linux, Rust 1.92+, Python 3.9+, Ruby 3.4+, Docker и OpenSSL.
Из корня репозитория:

```sh
python3 testbench/run.py demo
python3 testbench/run.py check
python3 testbench/run.py tcp
python3 testbench/run.py daemon
```

Runner поднимает отдельный NATS-контейнер с TLS-first, временными сертификатами
и credentials, затем удаляет его. `demo` запускает обмен; `check` проверяет
fmt/clippy, движок/codec и реальные транспортные сценарии.
[Первый запуск](docs/getting-started.md) содержит подготовку, ожидаемый результат,
режим с кэшем и диагностику.

В [документации](docs/README.md) есть
[архитектура со схемами](docs/architecture.md),
[контракт движка](docs/stream-engine.md), [wire v1](docs/wire.md),
[структура репозитория](docs/repository.md) и [концепция](docs/concept.md).

Dynamic NatsRuntime поддерживает broker-authorized join/rejoin, peer liveness и
recovery для новых streams. Готовые клиентские приложения/HTTP-SOCKS-VPN proxy,
transparent byte resumption, native FFI и мобильные адаптеры ещё не реализованы.
[Daemon/IPC v1](docs/daemon-ipc.md) реализован для Linux; выдача network identity/credentials остаётся host provisioning.
Логические бюджеты Core не являются гарантией RSS всего процесса. Текущие типы и wire остаются экспериментальными.

[Dynamic NATS runtime: join, recovery, trust, queues and embedding](docs/nats-runtime.md).
