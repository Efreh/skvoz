# Реальный NATS-стенд SKVOZ

Отдельный экспериментальный подпроект: package `skvoz-testbench`, driver/demo
в `src/`, реальные сценарии в `tests/`, контейнерный runner в `run.py`.
Использует [ядро](../core/README.md) через path dependency, один client/core
на каждую сторону, buffer connectors в одном Rust-процессе.

Из корня общего репозитория:

```sh
python3 testbench/run.py check
python3 testbench/run.py demo
```

Из этого каталога работают `python3 run.py check` и `python3 run.py demo`:
runner вычисляет общий корень по своему пути. Требуются Linux, Rust 1.92+,
Python 3.9+, доступ к Docker daemon и OpenSSL. Для cached dependencies/image
добавьте `--offline`; Compose и host NATS не нужны.

Runner поднимает отдельный pinned NATS с TLS-first/временными credentials,
ждёт готовности и удаляет свои контейнер/сертификаты после завершения,
включая failed tests. Режим `check` включает fmt/clippy и реальные проверки;
режим `demo` запускает двусторонний обмен.
Прямой Cargo запуск real_nats требует feature и runner-owned env;
основной entrypoint — `run.py`, успешного пропуска broker tests нет.

[Контракт движка](../docs/stream-engine.md),
[формат пакетов](../docs/wire.md),
[общая структура](../docs/repository.md).

[Текущая архитектура и схемы](../docs/architecture.md),
[первый запуск и диагностика](../docs/getting-started.md),
[указатель документации](../docs/README.md).
