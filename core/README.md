# SKVOZ Core

Подпроект ядра в общем репозитории SKVOZ. Rust package `skvoz-core`,
участник корневого workspace. Содержит std-only Stream/Manager без I/O, экспериментальный wire codec и
опциональный `nats` runtime (async-nats/Tokio). Все коннекторы и обе стороны
используют одну библиотеку и контракт.

`src/` — состояния, кредит, агрегатные бюджеты, multi-peer routing, очереди и codec; `tests/` — contract/fixture/wire
проверки; `examples/` — пример в памяти. [Контракт движка](../docs/stream-engine.md)
и [wire формат](../docs/wire.md) находятся в общей документации;
[fixtures/vectors](tests/fixtures/README.md) — рядом с тестами.

Команды из корня общего репозитория:

```sh
cargo test -p skvoz-core --locked
cargo run -p skvoz-core --example in_memory --locked
python3 testbench/run.py check
```

Последняя команда — основная проверка ядра вместе с настоящим транспортом.
Клиентские приложения принадлежат [clients/](../clients/README.md),
контейнерный runner и сценарии — [testbench/](../testbench/README.md).
NatsNode находится в библиотеке Core; [TCP relay](../connectors/tcp/README.md)
использует его как embedding host.
По умолчанию crate не имеет внешних зависимостей. Feature `nats` включает
статический NatsNode с явными routes и динамический NatsRuntime с authenticated
join/rejoin, lane readiness/liveness и generation-safe connector keys.
[Контракт Manager/NatsNode](../docs/stream-engine.md#менеджер-множества-потоков)
описывает admission, планирование и границы памяти.
Стабильный wire и FFI остаются будущей работой. Credential issuance/revocation и
certificate management принадлежат host/provisioner; runtime поддерживает
проверенные System/ManagedCa trust и provisioned username/password.
Общие правила: [структура репозитория](../docs/repository.md).

[Текущая архитектура и схемы](../docs/architecture.md),
[первый запуск и диагностика](../docs/getting-started.md),
[указатель документации](../docs/README.md).

[Dynamic NATS runtime: join, recovery, trust, queues and embedding](../docs/nats-runtime.md).

Для host languages доступен [standalone daemon/IPC v1](../docs/daemon-ipc.md),
который встраивает эту же Core library; Rust embedding API сохраняется.
