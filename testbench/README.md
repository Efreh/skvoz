# Реальный NATS-стенд SKVOZ

Отдельный экспериментальный подпроект: package `skvoz-testbench`, adapters/demo/load
в `src/`, реальные сценарии в `tests/`, контейнерный runner в `run.py`.
Использует [ядро](../core/README.md) через path dependency, reusable NatsNode. В legacy demo два участника; multi-owner сценарии создают
один server и несколько client nodes с отдельными credentials. В прежнем `load` app nodes работают в одном Rust-процессе; `qualify` запускает
независимые release server/client processes. Брокер находится в отдельном контейнере.

Из корня общего репозитория:

```sh
python3 testbench/run.py check
python3 testbench/run.py demo
python3 testbench/run.py tcp
python3 testbench/run.py load --clients 100 --streams-per-client 10 --active-per-client 2 --bytes 65536
```

Из этого каталога работают `python3 run.py check` и `python3 run.py demo`:
runner вычисляет общий корень по своему пути. Требуются Linux, Rust 1.92+,
Python 3.9+, доступ к Docker daemon и OpenSSL. Для cached dependencies/image
добавьте `--offline`; Compose и host NATS не нужны.

Runner поднимает отдельный pinned NATS с TLS-first/временными credentials,
ждёт готовности и удаляет свои контейнер/сертификаты после завершения,
включая failed tests. Режим `check` включает fmt/clippy и реальные проверки;
режим `demo` запускает двусторонний обмен. `tcp` проверяет реальные сокеты
с FIN запроса и ответом после EOF. `load` проверяет одинаковые IDs разных
клиентов, смешанный трафик, медленного читателя и очистку idle slots.
Параметры и область измерения: [руководство](../docs/getting-started.md#нагрузочный-эксперимент).
Число logical streams не создаёт отдельные NATS соединения или async tasks.
Прямой Cargo запуск real_nats требует feature и runner-owned env;
основной entrypoint — `run.py`, успешного пропуска broker tests нет.

[Контракт движка](../docs/stream-engine.md),
[формат пакетов](../docs/wire.md),
[общая структура](../docs/repository.md).

[Текущая архитектура и схемы](../docs/architecture.md),
[первый запуск и диагностика](../docs/getting-started.md),
[указатель документации](../docs/README.md).

`qualify` проверяет dynamic NatsRuntime, authenticated new generations, independent
processes, bounded transport delay/slow-credit/churn и отдельные process metrics.
Команды, finite workload bounds и measurement scope: [qualification guide](../docs/getting-started.md#независимые-процессы-runtime).
Сырые metrics/credentials временные; capacity/SLA не заявляются.

## Standalone daemon и foreign languages

```sh
python3 testbench/run.py daemon --offline
```

Нужны Linux/Ruby3.4+ помимо основных prerequisites. Runner строит release
`skvoz-core-daemon`, запускает independent daemons и Python acceptor/Ruby/Python
stdlib clients через тот же dedicated TLS-first broker. `check` также включает
этот сценарий. Temporary ACL даёт одному device login только явно provisioned
PeerId1/2; это не automatic ID allocation и не credential isolation между ними.

[IPC contract](../docs/daemon-ipc.md) описывает finite profiles, watermark/credit,
owner admission/isolation и startup/lifecycle. Qualification проверяет tiny local
output queues отдельно от NATS subscription overflow tests, делает real broker
stop/start и daemon/client kill; source-only fixtures находятся в daemon/tests.
RSS/idle samples имеют ограниченный workload scope и не являются SLA.
