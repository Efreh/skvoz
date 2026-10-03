# Движок байтового потока

Документ описывает экспериментальный внутренний API `skvoz-core`.
Стабильные wire/FFI/IPC интерфейсы пока не определены.

## Модель и операции

Один `Stream` относится к одной сессии. Он не хранит сетевые идентификаторы;
драйвер обязан проверять identity/маршрутизацию до `receive`. Движок не
выполняет I/O, callbacks и фоновые задачи. Все вызовы последовательны,
через эксклюзивный mutable доступ.

- `open(metadata, now_ms)` начинает исходящее открытие и ставит OPEN.
- `receive(frame, now_ms)` валидирует фрейм и применяет его. Полученный OPEN в Idle создаёт входящую попытку и IncomingOpen.
- `accept(metadata)` / `reject(reason)` разрешены только для входящей попытки.
- `send(bytes)` возвращает `Accepted(n)` либо `WouldBlock`; возможна частичная отправка, остаток принадлежит вызывающему.
- `finish()` отмечает EOF локального отправителя, повтор идемпотентен.
- `consume_through(offset)` подтверждает непрерывно потреблённый префикс только уже выданных коннектору байтов. Повтор/устаревшее подтверждение ничего не меняет.
- `poll_frames(max_count)` передаёт владение исходящими фреймами драйверу; он обязан сохранить их порядок и ограничить собственные очереди.
- `poll_events(max_count)` передаёт владение событиями/буферами коннектору, поддерживает batch; 0 ничего не извлекает, один вызов выдаёт максимум 256 событий даже при большем max_count.
- `close(reason)` аварийно закрывает обе стороны и ставит CLOSE независимо от DATA-очереди; повтор ничего не меняет.
- `transport_lost()` закрывает локально без попытки отправить CLOSE по утраченному транспорту.
- `tick(now_ms)` проверяет opening deadline. Драйвер вызывает его регулярно и до операций коннектора; `receive` тоже проверяет время. Убывающее время/переполнение deadline — ошибка вызова.

Состояния: Idle, Opening (incoming/outgoing), Open, HalfClosedLocal, HalfClosedRemote, Draining (оба FIN, ожидается выдача/потребление данных), Closed. Во входящем Opening данные ещё недопустимы. Успешный accept переводит принимающий движок в Open; инициатор открывается только от ACCEPT. Очередь выдаёт ACCEPT раньше любого DATA.

## Диаграмма состояний

Схема показывает открытие и нормальное закрытие одного `Stream`.
Входящая и исходящая попытки открытия различаются направлением.

```mermaid
stateDiagram-v2
    state "Opening (outgoing)" as Outgoing
    state "Opening (incoming)" as Incoming
    [*] --> Idle
    Idle --> Outgoing: open
    Idle --> Incoming: receive OPEN
    Outgoing --> Open: receive ACCEPT
    Incoming --> Open: accept
    Outgoing --> Closed: REJECT / opening timeout
    Incoming --> Closed: reject / opening timeout
    Open --> HalfClosedLocal: finish
    Open --> HalfClosedRemote: receive FIN
    HalfClosedLocal --> Draining: receive FIN, drain pending
    HalfClosedLocal --> Closed: receive FIN, drain complete
    HalfClosedRemote --> Draining: finish
    Draining --> Closed: local FIN dispatched, incoming consumed
    Closed --> [*]
```

`drain complete` означает, что локальный FIN уже передан драйверу, исходящая
DATA-очередь пуста и все полученные байты потреблены. При получении второго FIN
поток может сразу перейти из HalfClosedLocal в Closed без наблюдаемого Draining.
Из любого незакрытого состояния возможен аварийный переход в Closed при
`close`, `transport_lost` или ошибке peer-протокола; эти переходы опущены для
читаемости. Closed терминален: late frames игнорируются, повторного открытия нет.

## Типизированные фреймы

| Фрейм | Поля / смысл |
| --- | --- |
| OPEN, ACCEPT | receive_window (u32), max_frame (u32), непрозрачные metadata. Объявляют лимиты приёма отправителя. |
| REJECT | Ограниченная непрозрачная причина. |
| DATA | offset (u64), непустые байты; offset первого DATA = 0. |
| WINDOW_UPDATE | consumed (u64), абсолютное число потреблённых байтов от начала направления. |
| FIN | final_offset (u64), точное число байтов направления до EOF. |
| CLOSE | Структурированная конечная причина. |

OPEN/ACCEPT валидируют положительное окно, `0 < max_frame <= receive_window`, верхние пределы и метаданные **до** копирования/изменения состояния. Один peer может иметь другие лимиты; send ограничивается меньшим local/remote max_frame. Экспериментальный сетевой формат описан в [wire.md](wire.md).

## Порядок и кредит

В каждом направлении движок хранит счётчики принятых к отправке, выданных транспорту, полученных, выданных коннектору и потреблённых байтов. Счётчики u64 не оборачиваются.

- DATA принимается только при `offset == received`; любой gap/duplicate — ProtocolError, никакой неограниченной reorder-очереди.
- FIN принимается только при `final_offset == received`. Повтор того же FIN идемпотентен; DATA после FIN недопустим.
- Кредит отправителя = объявленное peer окно минус (принятые к отправке − подтверждённые peer потреблённые). Резервирование происходит при send, не при poll_frames.
- WINDOW_UPDATE не может подтверждать больше байтов, чем уже выдано транспорту. Монотонный максимум подтверждения защищает от двойного/устаревшего кредитования.
- Получатель принимает не более `receive_window` непотреблённых байтов, включая уже выданные коннектору. `poll_events` не равен потреблению.
- `consume_through` не может превышать границу выданных Data. Коннектор обязан реально освободить соответствующие буферы; его копии и собственные очереди не являются памятью движка.
- WINDOW_UPDATE коалесцируется до последней границы; отдельное управляющее место обеспечивает прогресс даже при полной исходящей DATA-очереди.
- После WouldBlock переход к возможности send создаёт coalesced Writable. Движок не создаёт событие на каждое пополнение кредита.

## Буферы и численные пределы

| Параметр | Значение по умолчанию | Допустимый диапазон |
| --- | --- | --- |
| receive_window | 65 536 байт | 1…16 777 216 байт |
| max_frame | 16 384 байт | 1…65 536 байт, не больше окна |
| max_pending_frames | 64 DATA | 1…1 024 DATA |
| max_metadata | 4 096 байт | 0…65 536 байт |
| open_timeout_ms | 5 000 мс | положительный u64; сложение deadline проверяется |

При дефолтах внутренние полезные данные ≤ 65 536 + 64 × 16 384 + 3 × 4 096 = 1 126 400 байт на движок, плюс ограниченные структуры/allocator overhead. Три metadata-области — handshake frame и максимум два lifecycle event (IncomingOpen и Opened). Receive buffer начинается с нулевого capacity и растёт геометрически по фактически
пришедшим байтам, с reported capacity не больше receive_window. Реализация может
удерживать выделенный capacity после нормального drain; при закрытии освобождает byte queues. Эти числа **не являются пределом RSS процесса**, не включают кадры/события, уже переданные вызывающему, и не определяют глобальный бюджет множества потоков.

Метод проверки: snapshots occupancy/counters после каждого шага и тесты на наполненной очереди, все байты окна, множество однобайтовых DATA и Data, удерживаемые коннектором. Payload копируется только после проверки размера/кредита. Outgoing DATA имеет также предел количества, чтобы мелкие send не создавали неограниченные заголовки. Входящие DATA агрегируются в одном bounded byte buffer, а поллинг выдаёт chunks не больше max_frame.

Управляющее состояние конечно: один OPEN/ACCEPT, один накопительный WINDOW_UPDATE, один FIN, один аварийный terminal frame, flags lifecycle/Writable/RemoteFinished и один Closed. FIN выдаётся после всех ранее принятых DATA. CLOSE/REJECT заменяет непереданные DATA при аварийном прекращении.

## Закрытие, события и ошибки

FIN закрывает только отправляющее направление, противоположное может продолжать работу. RemoteFinished выдаётся после всех предыдущих Data в направлении, даже если они ещё не потреблены. Когда оба EOF известны, локальный FIN выдан транспорту и все входящие байты потреблены, движок переходит в Closed(Finished). Его событийный порядок — lifecycle, Writable, Data, RemoteFinished, Closed; соседние DATA могут агрегироваться.

Cancel, transport loss, protocol error и opening timeout отменяют непереданные/невыданные DATA и управляющие уведомления, очищают внутренние ресурсы и создают ровно один Closed. Abort может отбросить ещё не выданный IncomingOpen/Opened/RemoteFinished. Полученный REJECT создаёт Rejected и Closed(Rejected). Извлечённые ранее буферы остаются у вызывающего, но после abort consume недопустим. Late frames в Closed игнорируются, движок не переоткрывается; для новой сессии нужен новый экземпляр.

Некорректная операция локального API возвращает структурированную ошибку, сохраняя протокольное состояние. Некорректный удалённый фрейм возвращает ProtocolError, аварийно закрывает поток и ставит CLOSE(ProtocolError). Чужой/неаутентифицированный фрейм должен отсечь драйвер. Возобновление, liveness watchdog открытого потока, graceful shutdown драйвера и безопасность NATS находятся вне ответственности этого движка.

## Проверка

Semantic fixtures используют текстовые команды и hex payloads: другая
реализация может воспроизвести тот же сценарий. Они фиксируют поведение
движка. Бинарные wire vectors хранятся отдельно. Формат и тестовые данные:
[core/tests/fixtures](../core/tests/fixtures/README.md).

Из корня проекта:

```sh
cargo test -p skvoz-core --locked
python3 testbench/run.py check
```

Первый набор проверяет состояния, пределы и decoder; второй проверяет полный
путь через настоящий NATS. Ограниченный mutation smoke не заменяет длительный
fuzzing; полный предел RSS процесса и production transport требуют отдельных
измерений и проектирования.


## Менеджер множества потоков

`Manager` из той же crate — sans-I/O слой над Stream. Host регистрирует доверенный
`PeerId` через `register_peer(id, local_origin_bit)` и привязывает его к одной
аутентифицированной peer session. `StreamKey { peer, stream_id }` различает потоки
разных owners с одинаковым числовым ID. API использует ключ: `open(peer, metadata,
now_ms)`, accept/reject/send/finish/consume_through/close, `receive(key, frame,
now_ms)`, tick и batch poll. `RoutedFrame`/`ManagedEvent` возвращают ключ и payload.

| ManagerConfig | Default | Единица / смысл |
| --- | --- | --- |
| max_peers | 128 | Registered peers |
| max_streams | 8192 | Все live и closing entries |
| max_streams_per_peer | 128 | Entries одного peer |
| receive_budget | 67 108 864 | Байты обещанных receive windows |
| receive_budget_per_peer | 2 097 152 | Обещанные receive bytes одного peer |
| send_budget | 8 388 608 | Байты невыданных outgoing DATA |
| send_budget_per_peer | 262 144 | Невыданные outgoing DATA одного peer |
| stream | окно 8192, frame 1024, pending 8, metadata 256, timeout 5000 | Общая Config всех admitted streams |

Counts/send budgets должны быть положительны; receive budgets должны вмещать
хотя бы одно окно. Произведения slot count × window проверяются на overflow.
Config Stream валидируется до создания Manager. Эти defaults — настройки
экспериментального API, не capacity/throughput гарантии.

Admission проверяет независимо peer/global slots и полную статическую receive
reservation **до создания Stream или копирования metadata**. Reservation
сохраняется для closing entry до полной выдачи terminal events/frames; уменьшение
payload occupancy не позволяет переобещать кредит. Receive capacity zero на idle
не отменяет кредит, который peer уже вправе использовать. Local overload возвращает
Admission; remote OPEN получает не более одного pending REJECT на peer без
metadata. Остальные запросы при overload могут истечь по opening timeout.

`send` ограничен stream credit/frame queue и свободным peer/global send budget
до копирования payload. Может принять часть bytes. `poll_frames` освобождает
Manager send bytes, передавая allocation transport caller; они больше не входят
в этот counter. После освобождения бюджета блокированные streams получают
coalesced Writable. Wake обработка ограничена 32 waiters за poll/освобождение
бюджета, с cursor, поэтому host должен продолжать poll при большой очереди.

Ready peers и streams планируются вложенным round-robin: один frame на peer за
цикл, разные ready streams этого peer по очереди. Pending overload REJECT
чередуется с его stream work. Poll выдаёт не больше 256 элементов. Это frame
fairness; размер frames может различаться. Opening deadlines индексированы,
полного сканирования idle table на send/turn нет. `resources()` — явная
O(stream count) инспекция, не hot path driver.

Terminal entry удаляется после всех событий/фреймов. Closed IDs не открываются
повторно: per-peer local/remote monotonic high-water counters дают bounded replay
state. Unknown non-OPEN frame игнорируется. `peer_lost(peer)` запрещает новый
work этого peer и освобождает его внутренние queues; host должен выдать terminal
события. `remove_peer` допустим только для failed peer без entries/rejection.
Повторная registration означает новую authenticated session; нельзя сбрасывать
high-water для прежней сессии. `transport_lost` навсегда закрывает Manager.

`Resources` различает reservations, pending send, buffered receive, reported
receive capacity и unconsumed receive (включая уже выданные Data), ready peers и
pending rejections. Данные в событии принадлежат caller; caller обязан освободить
полностью или соответствующий потреблённый prefix **до** consume_through.
Abort не может отобрать ранее переданные allocations. Логические бюджеты не
включают allocator metadata, transport/runtime buffers, соединения и broker.

## Статический NatsNode

Feature `nats` экспортирует `nats::NatsNode`, `NatsConfig`, `PeerRoute` и
`FailureKind`. Host задаёт url/CA/credentials, namespace, local identity/session,
точные peer/session routes, queue capacities и per-turn limits. Для каждого
lifetime требуется новая generation. Routing envelope описан в [wire](wire.md).
Проверка CA, TLS и аутентификация обязательны по
[профилю транспорта](nats-runtime.md#профиль-транспорта); broker ACL должен разрешать
publish только с credential-bound sender и нужными recipient identities.

Subscription/client capacities — 1…65 536 сообщений/commands; input/output
per turn — 1…256 сообщений; io_timeout положителен. Namespace состоит из
непустых dot-separated tokens, token/session <=64 ASCII букв/цифр/`_`/`-`,
namespace <=256 байт. Duplicate/self peers отвергаются. Broker max_payload должен
поддерживать полный packet limit. NatsConfig не задаёт implicit credentials
или автоматический discovery.

`open/accept/reject/send/finish/consume_through/close` ставят работу; `turn(wait)`
передаёт bounded output batch, принимает bounded input и обрабатывает deadlines.
На пакет выполняется `flush`: в async-nats 0.50 он завершает локальную запись
буферов сокета, не подтверждая обработку брокером или потребление удалённой стороной.
`flush_pending()` передаёт один bounded output batch без inbound. `poll_events`
выдаёт terminal events и применяет async failure latch. Disconnect, overflow,
server/client error навсегда закрывают node при следующем owner turn/API/poll;
async-nats reconnect не возобновляет старые streams. `shutdown` завершает local
manager, unsubscribe и bounded NATS drain.

Отмена future `turn`/`flush_pending` во время publish/flush терминальна: после
передачи владения frame transport нельзя безопасно восстановить его порядок.
Drop guard защёлкивает ClientError; следующий owner API/poll применяет transport
loss. Отмена idle input wait безопасна. При externally cancelled relay host
должен продолжить terminal poll либо удалить node. Это не remote liveness или
гарантия доставки последнего CLOSE/FIN.

Peer routes статические: текущий NatsNode не заменяет session во время работы.
После restart клиента с новой session принимающую сторону нужно пересоздать
с новыми routes. Старые recipient subjects не достигают новой subscription,
неизвестный sender/session игнорируется; автоматический restart/rejoin отсутствует.

NATS очереди имеют message-count caps. Для Core publish/подписки грубая верхняя
оценка queued payload — capacity × 65 564 байта на очередь, плюс сообщения в
обработке; структуры, protocol/TLS/read buffers и NATS runtime accounting
не входят в Manager budgets. Broker configuration должна ограничивать payload
и доступ к этой subscription. Чужие subjects/payloads могут занять очередь до
проверки sender; authentication/ACL остаются обязательной границей доверия.
Это не полный предел RSS. [Нагрузочный режим](getting-started.md#нагрузочный-эксперимент)
показывает измеряемую область и её ограничения.

`Manager.aggregate()` возвращает счётчики потоков, резервирования, отправки и готовых очередей за постоянное время. `resources()` подробно просматривает буферы за O(число потоков). Жизненный цикл динамических сессий, ключи RuntimeKey с проверкой поколения и транспортные отметки определены в [контракте NATS runtime](nats-runtime.md) поверх того же Stream.
