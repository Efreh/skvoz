# Первый запуск

Для развёртывания сервера TCP через GHCR/Compose или локальной сборки того же
Docker-образа используйте [руководство серверного коннектора](server-connector.md).
Ниже описан стенд проверки Core.

Этот пример запускает два участника в одном процессе Rust. Коннектор
пользователя и коннектор потребителя открывают потоки и обмениваются бинарными
буферами через настоящий контейнер NATS. Схема и границы компонентов описаны
в [архитектуре](architecture.md).

## Подготовка

Для контейнерного стенда нужны Linux, Rust 1.92 или новее, Python 3.9 или новее,
OpenSSL и Docker с работающим daemon, доступным текущему пользователю.
Для `check` и `daemon` также нужен Ruby 3.4+; GTK-зависимости для стенда не нужны.
Проверьте инструменты:

```sh
rustc --version
cargo --version
python3 --version
openssl version
docker info
```

Дальнейшие команды выполняются из корня checkout. Отдельный NATS на хосте и
Compose не требуются. При первом запуске runner может скачать NATS image и
Cargo-зависимости. Образ закреплён версией и digest в
[testbench/run.py](../testbench/run.py); версии crates — в корневом Cargo.lock.

## Двусторонний обмен

```sh
python3 testbench/run.py demo
```

Runner создаёт NATS с TLS, временными сертификатами и отдельными правами
пользователя и потребителя. Порты выбираются динамически и публикуются только
на loopback. Демонстрация передаёт по 32 768 байт в каждом направлении каждого
из четырёх потоков; потоки инициируются обеими ролями.

При успешном завершении вывод содержит `PASS real NATS`, описание обмена и
`Testbench container removed`; команда возвращает код 0. После запуска runner
удаляет свой контейнер и временные TLS/configuration files, включая обычные
ошибки тестов. Пароли и ключи генерируются для каждого запуска.

## Проверка проекта

```sh
python3 testbench/run.py check
```

Команда проверяет форматирование, clippy и тесты workspace, включая реальные
NATS-сценарии, many-client routing и реальные TCP failures. Проверяются бинарные данные, кредит и медленный потребитель,
ответ после EOF, отказ/отмена/тайм-аут, конкурентные потоки, лимиты и изоляция,
переполнение подписки, остановка брокера, ошибки TLS/аутентификации/прав и
некорректные пакеты, освобождение и переиспользование slots, session isolation,
TCP socket error и deadline с отменой обоих фактических Core nodes. Ошибки подготовки стенда завершают проверку с ошибкой.

Если образ и зависимости уже находятся в кэше, используйте:

```sh
python3 testbench/run.py check --offline
python3 testbench/run.py demo --offline
```

Из каталога `testbench/` те же команды доступны как `python3 run.py check`
и `python3 run.py demo`: runner сам определяет корень workspace.

## Реальные TCP-сокеты

```sh
python3 testbench/run.py tcp --offline
```

Requester передаёт 32 768 байт и закрывает отправляющее направление TCP.
Target читает до EOF, затем возвращает 65 536 байт и FIN. Пример проверяет
полные bytes/EOF на обеих сторонах и нулевые live slots обоих NatsNode.
`PASS real TCP` и код 0 означают выполнение этого сценария.
[Relay одного сокета](../connectors/tcp/README.md) описывает API и ограничения.

## Нагрузочный эксперимент

```sh
python3 testbench/run.py load --offline --clients 100 --streams-per-client 10 --active-per-client 2 --bytes 65536
python3 testbench/run.py load --offline --clients 100 --streams-per-client 100 --active-per-client 2 --bytes 65536
```

Параметры: clients 1…512, streams-per-client 1…512, total streams ≤65536,
active-per-client 0…streams-per-client, bytes 0…2 097 152 на направление каждого
активного потока. Это предел runner experiment, не предел продукта. Для idle
sweep задайте `--active-per-client 0 --bytes 0`. Phase deadline — 60 секунд;
на медленной машине увеличивайте нагрузку постепенно.

Один server принимает клиентов с отдельными NATS credentials и одинаковыми
числовыми stream IDs. Каждому stream резервируется 8192 байт receive credit;
idle payload capacity должен оставаться нулевым. Активные streams передают
детерминированные бинарные данные в обе стороны; один клиент задерживает
возврат кредита первые 31 rounds. Проверяется прогресс других peers. Затем
все idle streams отменяются через NATS и slots/budgets освобождаются.

`LOAD idle` печатает workload, время connect/handshake, receive reservations и
capacity. `LOAD complete` печатает проверенные payload bytes, traffic elapsed,
throughput и cleanup. Дополнительные строки показывают completion p50/p95/p99,
Linux app RSS/peak RSS и CPU ticks (traffic вместе с cleanup), broker `/varz` memory/CPU/connections/messages.
Это completion latency workload, не per-packet latency или CPU SLA.
Runner использует стандартный debug/dev Cargo build, без release optimization;
throughput/time нельзя трактовать как release benchmark.

App RSS включает **все client nodes и server node в одном процессе**, runtime,
TLS и caller buffers; это не server-only RSS. Брокер отдельно ограничен одним
CPU и 128 MiB контейнера; приложение и вся машина этими лимитами не ограничены.
Broker mem/CPU — моментальные `/varz` наблюдения, не полный peak или whole-machine
budget. Очереди настраиваются конечными, но логические counters Core не задают
полный предел RSS. NATS connection на каждый app node, а не на logical stream.

100 клиентов и 1000/10 000 streams в командах — воспроизводимые workload параметры.
Результат localhost run не устанавливает maximum capacity, production throughput,
поддержку машины с одним CPU/2 GiB или WAN/мобильных сценариев.

## Движок без Docker

Для проверки только состояний/codec достаточно Rust:

```sh
cargo test -p skvoz-core --locked
cargo run --locked -p skvoz-core --example in_memory
```

Пример в памяти передаёт бинарный запрос, завершает отправку запроса и получает
ответ в обратном направлении. Эти проверки дополняют реальные NATS-сценарии;
они сами по себе не проверяют транспорт.

## Если запуск не удался

| Симптом | Действие |
| --- | --- |
| Не найден cargo, docker или openssl | Установите недостающий инструмент и проверьте его доступность в PATH. |
| Docker daemon недоступен или отказал в доступе | Проверьте `docker info` от того же пользователя и доступ к daemon. |
| Не хватает image/crates при `--offline` | Повторите без `--offline` при доступной сети. |
| Ошибка TLS или готовности брокера | Повторите через runner, который создаёт согласованные CA, конфигурацию и credentials; проверьте работоспособность Docker. |

Legacy demo/load использует finite buffers в одном процессе и статический
NatsNode; минимальный TCP relay требует выделенного node. Dynamic runtime
поддерживает join/rejoin и новые streams после recovery. Прерванные потоки
не возобновляются. [Клиент Ubuntu](ubuntu-client.md) поставляется отдельно через deb. Подробнее:
[архитектура](architecture.md), [контракт](stream-engine.md),
[формат пакетов](wire.md).

## Независимые процессы runtime

```sh
python3 testbench/run.py qualify --offline --clients 129 --streams-per-client 2 --active-per-client 1 --bytes 32768 --duration 3
python3 testbench/run.py qualify --offline --clients 8 --streams-per-client 4 --active-per-client 2 --bytes 32768 --duration 1 --delay-ms 40 --slow-reader-delay-ms 250 --churn-rounds 2
```

`qualify` строит release binary, запускает один server и отдельный client process
на identity с provisioned recipient/sender/shard ACL. Credentials генерируются по
workload, фиксированного набора101 больше нет. И `load`, и `qualify` ограничены
1..512 clients/streams-per-client и65536 total streams; это bounds стенда, не Core
ceiling. Профили, лимиты и embedding API: [runtime contract](nats-runtime.md).

`--duration 1..60` удерживает idle streams/heartbeats после active transfer;
`--churn-rounds 1..10` перезапускает client processes при живом server.
`--delay-ms 0..200` передаёт настоящий TLS traffic через bounded TCP delay
forwarder с половиной указанной задержки в каждую сторону и одним held8KiB chunk;
chunk pacing влияет на результат. `--slow-reader-delay-ms 0..2000` задерживает
consume-through у client1, сохраняя его driver/heartbeats; совокупное расчётное
окно задержки ограничено60s. Это позволяет сравнить progress остальных peers.
`--max-app-rss-mib 64..4096` (default1024) — stop limit суммарного sampled app RSS.
Controller имеет конечные phase/total deadlines и уничтожает всех child processes
при failure/timeout. Runtime output/credentials и metrics files остаются temporary.

`QUALIFY` печатает JSON с отдельными server app, aggregate client samples,
broker peaks, completion p50/p95/p99 и final stream/reservation counters.
CPU — observed ticks с указанным clock rate; RSS sample interval25ms не доказывает
полный allocation peak. Сумма process peaks, особенно через churn waves, не является
одновременно занятой RAM. Broker имеет отдельные1CPU/128MiB limits; application/
whole host ими не ограничены. Числа workload не являются product users/CPU/RAM SLA.

`check` проверяет dynamic runtime перед legacy suite, которая в конце останавливает
брокер. TLS negatives используют temporary wrong-name/expired/untrusted certificates;
System trust positive запускается в isolated child с временным SSL_CERT_FILE,
без изменения системного trust store. Credential revocation/reprovision проверяется
reload настоящего брокера. Для обычного endpoint Core доверяет native roots;
эта isolated проверка квалифицирует путь native loader, не публичного CA issuer.

Qualification queue profile вычисляется для его собственного sender pattern:
8192-byte window как8×1024-byte frames плюс4 lifecycle/coalesced-credit slots,
умноженные на streams и максимальное число peers в shard, округлённые до power-of-two
(min256). Для129×2 server profile512; это не гарантия для arbitrary tiny DATA.
Derived capacity выше65536 отклоняется как workload-limit error. Маленькие queues
отдельно проверяются на честное failure/isolation; профиль не скрывает overflow.

## Сборка демона из исходников

Для language-independent Core executable нужны дополнительно Ruby3.4+ и Linux.
Из корня:

```sh
cargo build --release -p skvoz-daemon --locked
./target/release/skvoz-core-daemon --help
python3 testbench/run.py daemon
```

`daemon` использует dedicated pinned NATS с TLS, независимые release processes,
Python incoming host, Ruby/Python stdlib clients и общий login для двух явно
provisioned PeerId. Проверяет exact binary bytes/EOF/credits, owner/stale handles,
nonreading owner isolation, private endpoint/config, TLS/auth negatives и real
broker/daemon/client restarts. `check` включает эту qualification. С кэшами
добавьте `--offline`. Profiles/CA/socket/metrics остаются временными вне checkout.

Собственный запуск через provisioned private profile, socket policy/ошибки/лимиты
и точный binary contract: [daemon/IPC v1](daemon-ipc.md). Binary не выдаёт
credentials/PeerId, не разбирает HTTP/SOCKS и не возобновляет прежние streams.
