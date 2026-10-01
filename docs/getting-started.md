# Первый запуск

Этот пример запускает два участника в одном процессе Rust. Коннектор
пользователя и коннектор потребителя открывают потоки и обмениваются бинарными
буферами через настоящий контейнер NATS. Схема и границы компонентов описаны
в [архитектуре](architecture.md).

## Подготовка

Для контейнерного стенда нужны Linux, Rust 1.92 или новее, Python 3.9 или новее,
OpenSSL и Docker с работающим daemon, доступным текущему пользователю.
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

Runner создаёт NATS с TLS-first, временными сертификатами и отдельными правами
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

Параметры: clients 1…100, streams-per-client 1…100,
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

Стенд использует конечные буферы в одном процессе. Минимальный TCP relay
требует выделенного node; готовый proxy/VPN, клиентские приложения,
автоматическое обновление routes и возобновление потоков пока не реализованы. Подробнее:
[архитектура](architecture.md), [контракт](stream-engine.md),
[формат пакетов](wire.md).
