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
│   └── README.md       # правила; полных клиентских приложений пока нет
├── connectors/         # коннекторы внешнего I/O
│   └── tcp/            # package skvoz-tcp, relay одного сокета
│       ├── Cargo.toml
│       ├── src/
│       └── README.md
├── testbench/          # отдельный интеграционный стенд
│   ├── Cargo.toml      # package skvoz-testbench
│   ├── src/
│   ├── tests/
│   └── run.py          # контейнер NATS: check/demo/tcp/load/qualify/daemon
├── docs/               # архитектура и контракты
└── README.md           # вход в общий проект
```

Каждый подпроект хранит свои исходники, manifest/build config, тесты и
компонентные инструменты в собственном каталоге. README описывает назначение,
границы и команды. Общая документация описывает назначение компонентов,
их интерфейсы, ограничения и способы сборки/проверки.

`clients/<name>/` предназначен для законченного клиентского подпроекта со
своими инструментами и документацией. Новые SDK, коннекторы, node, сервисы
или другие компоненты размещаются отдельно по назначению после определения
границ. Готовые структуры для несуществующих компонентов не создаются.

Корень хранит общую конфигурацию, документы и координацию сборки. Rust-пакеты
добавляются явными members в корневой Cargo.toml и используют общий Cargo.lock.
Другие языки не требуют переноса под Rust-каталог или вложенного workspace.
Внутренние пакеты сложного подпроекта можно разнести на его подкаталоги.

## Проверка проекта

Из корня репозитория:

```sh
python3 testbench/run.py check
python3 testbench/run.py demo
```

Для запуска с заранее скачанными зависимостями/image добавьте `--offline`.

[Указатель документации](README.md) связывает инструкции запуска, текущую
архитектуру и контракты. [Концепция](concept.md) описывает направления развития.

Dynamic runtime/control contract: [NATS runtime](nats-runtime.md). Independent release-process qualification and its measurement scope: [getting started](getting-started.md#независимые-процессы-runtime).

Standalone executable/source configuration and Unix socket contract: [daemon/IPC](daemon-ipc.md).
