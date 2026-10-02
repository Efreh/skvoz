# Текущая архитектура

Документ описывает реализованные компоненты SKVOZ. Цели для production-узла,
клиентов и мобильных адаптеров находятся отдельно в [концепции](concept.md).

## Компоненты и границы

`skvoz-core` — одна универсальная Rust-библиотека для любых коннекторов,
client и server приложений. Stream/Manager/codec не выполняют I/O;
опциональный feature `nats` добавляет статический NatsNode и динамический NatsRuntime на Tokio/async-nats.
Роль приложения не меняет реализацию или контракт ядра.

Manager привязывает поток к `(PeerId, stream_id)`, резервирует полное объявленное
receive window при admission, ограничивает live/closing slots и pending DATA
bytes глобально и по peer. Receive payload buffer выделяется только при DATA;
у idle streams capacity равен нулю. Драйвер обслуживает ready queues и индекс
opening deadlines; нормальный turn не сканирует все idle streams.

Динамический [NatsRuntime](nats-runtime.md) поддерживает authenticated join/rejoin,
новые поколения после transport loss и lane PING/PONG proof перед peer-ready.
Он использует одну join connection и bounded lazy transport shards, без задач на
каждый stream. `qualify` запускает отдельный release server и client processes;
перегрузка одной lane закрывает её peers, а остальные shards продолжают работу.

В прежнем `load` many-client стенде все client/server приложения находятся в одном процессе,
а брокер — в отдельном контейнере. В развёртывании приложения могут находиться
на разных машинах. Server application со встроенным Core и NATS могут делить
одну машину и её ресурсы. Стенд не доказывает capacity такой машины.

```mermaid
flowchart TB
    subgraph clients["Отдельные client application processes"]
        host["Коннекторы и caller buffers"]
        runtime["Та же Core library: NatsRuntime / Manager / Streams"]
        host <-->|"API / generation-safe events"| runtime
    end
    subgraph remote["Server host: процессы имеют отдельные ресурсы"]
        server["Server application: коннектор + та же Core library"]
        broker["Core NATS: TLS, credentials, sender/recipient/shard ACL"]
        server <-->|"Join connection + bounded lazy lanes"| broker
    end
    runtime <-->|"TLS: join connection + lazy lane connections"| broker
    legacy["Legacy demo/load: статические NatsNode в одном процессе"]
    legacy <-->|"TLS / статические routes"| broker
```

Каждый NatsNode владеет одним NATS client и одной подпиской, без отдельного
соединения или задачи на logical stream. Host задаёт явный список доверенных
peer identities и session generations. Subjects имеют вид
`<namespace>.<recipient>.<recipient-session>.<sender>.<sender-session>`.
NatsNode подписывается на свой recipient/session с wildcard sender и проверяет
точную зарегистрированную пару sender/session до передачи в Manager.
Credentials/ACL брокера должны связывать sender identity с разрешёнными
recipient subjects; session token сам по себе не является аутентификацией.

API коннектора синхронно ставит bounded work. `turn(wait)` кодирует и публикует
не больше настроенного числа фреймов, принимает bounded input batch и подаёт
монотонное время менеджеру. Flush выполняется на batch, а не на каждый send.
Будущий вызов turn обязателен для передачи enqueue операций. `flush_pending`
передаёт bounded output batch без чтения inbound. Поллинг событий также обязан
продолжаться, чтобы освободить terminal entries.

`connectors/tcp` владеет одним сокетом и выдаваемыми Core буферами. Реальный
TCP пример создаёт два выделенных NatsNode того же Core, локальный requester
и удалённый target. Он проверяет bytes/FIN/ответ после EOF. Для many-socket proxy
нужен connector dispatcher; текущий relay не является таким proxy.

## Открытие и возврат кредита

Обе стороны могут инициировать поток. Ниже один цикл для инициатора A и
принимающей стороны B; каждый транспортный фрейм проходит через NATS.
События извлекаются коннекторами, а входящие публикации — драйвером.

```mermaid
sequenceDiagram
    participant A as Коннектор A
    participant NA as NatsNode A / Manager
    participant N as Core NATS
    participant NB as NatsNode B / Manager
    participant B as Коннектор B
    A->>NA: open(metadata)
    NA->>N: OPEN с лимитами приёма A
    N->>NB: OPEN
    NB-->>B: IncomingOpen
    B->>NB: accept(metadata)
    NB->>N: ACCEPT с лимитами приёма B
    N->>NA: ACCEPT
    NA-->>A: Opened
    A->>NA: send(bytes)
    NA-->>A: Accepted(n)
    Note over NA: Последующий turn отправляет очередь
    NA->>N: DATA(offset, bytes)
    N->>NB: DATA
    NB-->>B: Data(offset, bytes)
    Note over NB,B: Извлечение Data не возвращает кредит
    B->>B: Потребить и освободить буфер
    B->>NB: consume_through(offset + n)
    NB->>N: WINDOW_UPDATE(consumed)
    N->>NA: WINDOW_UPDATE
    opt send ранее вернул WouldBlock и снова возможен
        NA-->>A: Writable
    end
```

`send` принимает не больше одного фрейма за вызов и может принять только часть
переданного буфера. Принятые байты сразу резервируют кредит. Оставшуюся часть
хранит коннектор. `poll_events` передаёт буфер коннектору, но не подтверждает
потребление: кредит возвращает только `consume_through`.

Событие `Writable` может также возникнуть после освобождения места в исходящей
очереди. Драйвер вызывает `poll_frames`, чтобы извлечь фреймы; этот вызов
передаёт владение драйверу и ещё не означает доставку peer. NATS `flush`
завершает локальную запись socket buffers; он не подтверждает broker-installed
subscriptions или потребление данных B. Динамический runtime подтверждает готовность
lane настоящим matching PING/PONG.

## Решение об отправке

Схема ниже описывает `Manager.send` поверх `Stream.send` после открытия потока. В ней показаны
условия частичной отправки, backpressure и ошибки API.

```mermaid
flowchart TD
    start["send(bytes)"] --> state{"Handshake завершён<br/>и local FIN не установлен?"}
    state -->|"нет"| error["InvalidState"]
    state -->|"да"| empty{"Буфер пуст?"}
    empty -->|"да"| zero["Accepted(0)"]
    empty -->|"нет"| room{"Есть кредит, место в DATA-очереди<br/>и peer/global send budget?"}
    room -->|"нет"| blocked["WouldBlock<br/>буфер остаётся у коннектора"]
    room -->|"да"| size["n = минимум размера буфера,<br/>local/peer max_frame, кредита<br/>и остатка send budget"]
    size --> offset{"offset + n помещается в u64?"}
    offset -->|"нет"| overflow["OffsetExhausted<br/>состояние не меняется"]
    offset -->|"да"| queue["Поставить DATA в очередь<br/>и зарезервировать n байт кредита"]
    queue --> accepted["Accepted(n)<br/>остаток остаётся у коннектора"]
```

Точные единицы, значения по умолчанию и границы определены в
[контракте движка](stream-engine.md), формат DATA — в [wire v1](wire.md).
Собственные очереди драйвера и коннектора также должны быть ограничены.

## EOF и аварийное закрытие

`finish` закрывает только локальное отправляющее направление. FIN извлекается
после ранее принятых DATA. Противоположная сторона может продолжать отправку,
в том числе ответ после EOF запроса.

Нормальное завершение требует обоих EOF, передачи локального FIN драйверу и
потребления всех входящих байтов. Manager удаляет terminal entry и освобождает
receive reservation только после выдачи всех terminal frames/events. Числовой
ID в той же peer session повторно не используется; новые monotonic IDs могут
занять освобождённые slots. Ошибка протокола, отмена, opening timeout или
потеря транспорта закрывают поток аварийно и освобождают внутренние очереди.
Ранее переданные вызывающему буферы остаются у вызывающего.
[Диаграмма состояний и события](stream-engine.md) описывают эти переходы.

У Node есть конечное состояние отказа транспорта: после его применения
локальные потоки закрыты, и старый Node больше не открывает новые. Восстановление
соединения библиотекой NATS не возобновляет эти потоки.

## Изоляция и пределы реализации

Runner выдаёт раздельные временные credentials каждой стороне и каждому
клиенту. CA, ключи и пароли создаются заново; NATS принимает TLS-first соединения.
Подробности: [первый запуск](getting-started.md).

Round-robin выдаёт один frame на ready peer, вращая streams внутри peer.
Ограниченные send budgets защищают admission/очереди; frame fairness не является
byte fairness или гарантией p99 latency. Полная receive reservation сохраняется
до terminal cleanup, включая выданные коннектору непотреблённые данные. Малое
окно экономит обещанный receive budget, но может ограничить throughput при RTT.

Disconnect/SlowConsumer/server/client error навсегда защёлкивает отказ NatsNode.
Следующий owner API/turn/poll применяет transport_lost; восстановление соединения
async-nats не восстанавливает потоки. `peer_lost` закрывает только заданного peer.
Отмена turn/flush_pending при передаче output защёлкивает ClientError, поскольку
извлечённый frame нельзя безопасно вернуть в ordered очередь. Следующий owner
API/poll закрывает node; idle input wait можно отменять без этого отказа.
Статические routes не обновляются во время работы: смена session generation
клиента требует пересоздания/перенастройки принимающего node. Старые recipient
subjects не доставляются новой subscription; stale sender session игнорируется.
Эти ограничения относятся к статическому NatsNode. Динамический NatsRuntime
поддерживает authenticated join/rejoin и bounded recovery для новых streams;
transparent byte resumption не реализовано.

Статический NatsNode не обнаруживает тихий исчезнувший peer или потерю последней
публикации. NatsRuntime обнаруживает их nonce heartbeat и applied-frame watermark. Логические byte counters не включают
allocator overhead, externally retained frames/events, connector queues,
NATS/TLS/runtime buffers или broker. RSS нагрузки включает все clients и server
в одном процессе; ограничения контейнера относятся только к брокеру. Короткий
loopback эксперимент не доказывает WAN, mobile или whole-machine SLA.

Исходники: [Stream](../core/src/stream.rs), [Manager](../core/src/manager.rs),
[codec](../core/src/wire.rs), [NatsNode](../core/src/nats.rs),
[NatsRuntime](../core/src/runtime.rs),
[TCP relay](../connectors/tcp/src/lib.rs), [нагрузка](../testbench/src/mesh.rs).
