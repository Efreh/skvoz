# SKVOZ Core

Подпроект ядра в общем репозитории SKVOZ. Rust package `skvoz-core`,
участник корневого workspace. Сейчас содержит std-only движок потока
без I/O/runtime и экспериментальный wire codec.

`src/` — состояния, кредит, очереди и codec; `tests/` — contract/fixture/wire
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
экспериментальный NATS runtime/runner — [testbench/](../testbench/README.md).
Стабильный публичный wire, production node и FFI остаются будущей работой.
Общие правила: [структура репозитория](../docs/repository.md).

[Текущая архитектура и схемы](../docs/architecture.md),
[первый запуск и диагностика](../docs/getting-started.md),
[указатель документации](../docs/README.md).
