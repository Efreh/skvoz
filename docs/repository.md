# Структура общего репозитория SKVOZ

Репозиторий содержит весь модульный проект — ядро, клиенты и остальные
компоненты. Исходники, конфигурация и тесты каждого подпроекта находятся
в его собственном каталоге.

```text
skvoz/
├── Cargo.toml          # общий Rust workspace
├── Cargo.lock          # зависимости Rust-компонентов
├── core/               # подпроект ядра, package skvoz-core
│   ├── Cargo.toml
│   ├── src/
│   ├── tests/
│   └── examples/
├── daemon/             # process/IPC wrapper той же Core library
│   ├── Cargo.toml      # skvoz-daemon / skvoz-core-daemon binary
│   ├── src/
│   └── tests/fixtures/ # public IPC vectors
├── clients/            # каждый клиент — clients/<name>/
│   ├── python/         # stdlib IPC helper/echo example
│   ├── ruby/           # stdlib IPC helper/echo example
│   ├── ubuntu/         # Rust/GTK: src, tests, spec, packaging
│   └── README.md       # нативный клиент и самостоятельные IPC-примеры
├── connectors/         # коннекторы внешнего I/O
│   ├── server/         # Ruby host, Gemfile/lock, bin/lib/spec, Docker/Compose
│   └── tcp/            # package skvoz-tcp, relay одного сокета
│       ├── Cargo.toml
│       ├── src/
│       └── README.md
├── testbench/          # отдельный интеграционный стенд
│   ├── Cargo.toml      # package skvoz-testbench
│   ├── src/
│   ├── tests/
│   └── run.py          # контейнер NATS: check/demo/tcp/load/qualify/daemon
├── .github/workflows/  # раздельные сборки сервера и Ubuntu
├── docs/               # архитектура и контракты
└── README.md           # вход в общий проект
```

Каждый подпроект хранит свои исходники, манифест и конфигурацию сборки, тесты и
компонентные инструменты в собственном каталоге. README описывает назначение,
границы и команды. Общая документация описывает назначение компонентов,
их интерфейсы, ограничения и способы сборки/проверки.

`clients/<name>/` содержит клиентское приложение или самостоятельный адаптер IPC
со своими инструментами и документацией. Python/Ruby — примеры адаптеров,
Ubuntu — законченное настольное приложение. Новые SDK, коннекторы, node, сервисы
или другие компоненты размещаются отдельно по назначению после определения
границ. Готовые структуры для несуществующих компонентов не создаются.

Корень хранит общую конфигурацию, документы и координацию сборки. Rust-пакеты
добавляются явными members в корневой Cargo.toml и используют общий Cargo.lock.
Другие языки не требуют переноса под Rust-каталог или вложенного workspace.
Внутренние пакеты сложного подпроекта можно разнести на его подкаталоги.

## Где менять поведение

| Ответственность | Основные исходники |
| --- | --- |
| Состояния потока, кредит и EOF | `core/src/stream.rs`, `types.rs` |
| Допуск, бюджеты и справедливое планирование | `core/src/manager.rs` |
| Бинарные фреймы | `core/src/wire.rs`, `core/tests/fixtures/` |
| Публичные типы и проверка профиля NATS без I/O | `core/src/runtime/api.rs` (экспорт через `runtime`) |
| Динамические сессии, каналы и восстановление NATS | `core/src/runtime.rs`, `runtime_tls.rs` |
| Статическое встраивание NATS | `core/src/nats.rs`, `connectors/tcp/src/lib.rs` |
| Локальные владельцы и обслуживание Core | `daemon/src/driver.rs`, `endpoint.rs` |
| Профиль демона и IPC | `daemon/src/config.rs`, `protocol.rs`, `daemon/tests/fixtures/` |
| Сервер: процессы, пользователи, TLS | `connectors/server/lib/skvoz/server/{service,process,state,tls,enrollment}.rb` |
| Сервер: назначения, TCP, очереди и IPC | `connectors/server/lib/skvoz/server/{destination,policy,tcp_connector,budget,ipc_session,ipc_protocol}.rb` |
| Ubuntu: подключение, выделение устройства и настройки | `clients/ubuntu/src/{backend,enrollment,settings}.rs` |
| Ubuntu: протокольные интерфейсы и локальный IPC | `clients/ubuntu/src/{proxy,ipc}.rs` |
| Ubuntu: запуск приложения и связь UI, backend и desktop | `clients/ubuntu/src/app.rs` |
| Ubuntu: окно, фон, индикатор и журнал | `clients/ubuntu/src/{ui,desktop,tray,telemetry}.rs` |
| Реальный NATS и межпроцессные сценарии | `testbench/run.py`, `testbench/{qualification,daemon_qualification}.py`, `testbench/src/`, `testbench/tests/` |

IPC-адаптеры в разных языках реализуют один [локальный контракт](daemon-ipc.md),
а не отдельные ядра. Статический NatsNode используется примерами и регрессионными
сценариями; сервер и Ubuntu используют динамический NatsRuntime внутри демона.
Стенд и его адаптер фиксированной пары не входят в зависимости приложений.

## Проверка проекта

Из корня репозитория:

```sh
python3 testbench/run.py check
python3 testbench/run.py demo
```

Для запуска с заранее скачанными зависимостями и образами добавьте `--offline`.

[Указатель документации](README.md) связывает инструкции запуска, текущую
архитектуру и контракты. [Концепция](concept.md) описывает направления развития.

Контракт динамического runtime и управления: [NATS runtime](nats-runtime.md).
Проверка в независимых процессах сборки release и границы измерений:
[первый запуск](getting-started.md#независимые-процессы-runtime).

Сборка и конфигурация отдельного исполняемого ядра, контракт Unix-сокета:
[демон и IPC](daemon-ipc.md).

Сервер TCP со всеми компонентами, единый Dockerfile для локальной сборки и CI/GHCR:
[серверный коннектор](server-connector.md).

Нативный [клиент Ubuntu](ubuntu-client.md) входит в общую Cargo workspace и Cargo.lock, но собирается и проверяется отдельным workflow; основной стенд исключает GUI-пакет. Серверный Docker-build читает только его manifest и не собирает GTK.

Значения по умолчанию библиотеки и профили приложений имеют разные задачи.
[Manager](stream-engine.md#менеджер-множества-потоков) использует небольшие окна;
сервер и Ubuntu задают окна 1 МиБ и блоки 32 КиБ с согласованными бюджетами очередей.
Это настройки коннекторов поверх общего Core, а не отдельные варианты ядра.
