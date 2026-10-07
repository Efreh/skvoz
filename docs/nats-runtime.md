# Динамический NATS runtime

`skvoz_core::runtime::NatsRuntime` — опциональный runtime одной универсальной
Core library 4.0.1. Он добавляет authenticated join, смену peer session, проверку
живости и восстановление транспорта для новых byte streams. Включается feature
`nats`; HTTP/SOCKS parsing, DNS, сокеты, GUI и выдача credentials принадлежат host.
Статический [NatsNode](../core/src/nats.rs) используется простым TCP relay
и статическими сценариями стенда. У него нет автоматического join/recovery.

## Подключение из приложения

```rust
use skvoz_core::{Config, ManagerConfig, PeerId};
use skvoz_core::runtime::{Authentication, Membership, NatsRuntime, RuntimeConfig, Trust};
use std::collections::BTreeSet;

async fn connect() -> Result<NatsRuntime, Box<dyn std::error::Error>> {
    let auth = Authentication {
        username: std::env::var("NATS_USERNAME")?,
        password: std::env::var("NATS_PASSWORD")?,
};
let mut config = RuntimeConfig::new(
    "tls://nats.example.org:4222", Trust::System, auth,
    "skvoz.application", PeerId(1),
    Membership::Allowlist(BTreeSet::from([PeerId(0)])),
);
config.initiate = vec![PeerId(0)];
let limits = ManagerConfig {
    stream: Config { max_metadata: 512, ..ManagerConfig::default().stream },
    ..ManagerConfig::default()
};
Ok(NatsRuntime::connect(config, limits).await?)
}
```

Компилируемый пример встраивания: [runtime_profile.rs](../core/examples/runtime_profile.rs).
Приложение вызывает `turn` и извлекает события, прежде чем полагаться на `peer_ready`.
`RuntimeConfig::validate_profile(limits)` проверяет профиль без I/O и возвращает
настроенную границу транспортной нагрузки. [Отдельный демон с IPC](daemon-ipc.md)
использует тот же runtime для приложений на других языках.

`Trust::System` использует native trust store. `ManagedCa(PathBuf)` использует
проверенный app-specific PEM bundle, без глобальной установки корня.
Проверка цепочки, срока и hostname обязательна. Runtime принимает provisioned
username/password; JWT/NKey credentials в этом профиле не поддерживаются.
Передача профиля, хранение/отзыв credentials, первоначальное доверие и обновление
сертификата — обязанности provisioner/host. TLS не выдаёт сертификаты и не
позволяет доверять произвольному скачанному CA. URL с embedded userinfo запрещён;
Debug конфигурации и typed errors не раскрывают endpoint или пароль.

## Профиль транспорта

Подключение выполняется в обычном порядке NATS `INFO → TLS → CONNECT` во всех
соединениях: join, transport lanes и восстановление. Открытым остаётся только
начальный `INFO` с метаданными брокера. Пароль и payload передаются после
проверенного TLS; `require_tls(true)` запрещает открытый транспорт.
Брокер должен отправлять `INFO` до TLS (`tls.handshake_first: false`).
Брокер с другим порядком приветствия требует согласованного изменения
конфигурации. Проверка TLS обязательна; автоматического перехода на открытый транспорт нет.

`RuntimeConfig.tls_server_name: Option<String>` задаёт ожидаемую TLS-идентичность
со значением `None` по умолчанию. Приложение может подключаться к
`tls://127.0.0.1:4222`, проверяя явно заданный публичный IP-адрес или домен
в SAN сертификата. Штатный `WebPkiServerVerifier` по-прежнему проверяет
цепочку сертификатов, срок действия, SAN и подписи при установлении соединения;
меняется только ожидаемое имя или IP-адрес. `System` загружает системные корни
и прекращает подключение при ошибке загрузки. `ManagedCa` проверяет доверие
только по переданным корням PEM; пустые, недоверенные или некорректные корни
приводят к отказу. В текущем async-nats 0.50 загрузка системных корней
выполняется также до применения собственной конфигурации TLS, поэтому
повреждённое системное хранилище может помешать даже режиму `ManagedCa`.
Это безопасный отказ без перехода к посторонним корням доверия.
Параметр не меняет SNI из URL и не добавляет маршрутизацию TLS
по виртуальным хостам. Без параметра сохраняется проверка имени из URL.
[Профили демона](daemon-ipc.md) предоставляют то же необязательное поле.

NATS должен объявлять `max_payload: 65588`: максимальный wire v2 packet 65564
bytes плюс runtime envelope 24 bytes. Runtime отвергает и меньший, и больший
лимит, чтобы count-bounded inbound queues имели конечную границу payload bytes.
При общем брокере этот профиль необходимо согласовать с другими приложениями.

## API коннектора

Обе стороны используют один интерфейс. Операции над потоком синхронно ставят
работу в ограниченные очереди; приложение обслуживает её через async `turn`.

| Задача приложения | Метод |
| --- | --- |
| Подключить runtime с профилем TLS/auth и лимитами | `NatsRuntime::connect(config, limits).await` |
| Начать соединение с разрешённым peer / проверить его готовность | `join_peer(peer)` / `peer_ready(peer)` |
| Открыть поток, передав непрозрачные metadata назначения | `open(peer, metadata)` → `RuntimeKey` |
| Принять или отклонить входящий поток | `accept(key, metadata)` / `reject(key, reason)` |
| Поставить байты на отправку | `send(key, bytes)` → `Accepted(n)` или `WouldBlock` |
| Получить события потока | `poll_events(max)` → `Vec<RuntimeEvent>` |
| Ограничить передачу DATA доступной ёмкостью host | `poll_events_with_data_budget(max, admit)` |
| Подтвердить обработку полученных байтов | `consume_through(key, end_offset)` |
| Завершить отправляющее направление / отменить поток | `finish(key)` / `close(key)` |
| Обслужить транспорт, negotiation и таймеры | `turn(wait).await` |
| Прервать только ожидание idle-транспорта по готовности внешнего I/O | `turn_with_wake(wait, wake).await` |
| Прочитать локальный профиль / проверенные лимиты удалённого потока | `limits()` / `peer_limits(key)` |
| Получить состояние / подробные данные о буферах | `status()` / `resources()` |
| Завершить peer / отозвать его локальное разрешение | `terminate_peer(peer)` / `revoke_peer(peer)` |
| Завершить работу runtime | `shutdown().await` |

`RuntimeEvent` содержит ключ и событие, например `IncomingOpen`, `Opened`, `Data`,
`Writable`, `RemoteFinished` или `Closed`. После `IncomingOpen` серверный коннектор
сам открывает целевой сокет и подтверждает поток через `accept`. Кодирование
адреса в metadata принадлежит коннекторам.

`Accepted(n)` может обозначать только часть предложенного буфера: остаток хранит
приложение. Получение `Data` не возвращает кредит. `consume_through` подтверждает
абсолютную конечную позицию действительно обработанных байтов. `finish` допускает
ответ второй стороны после EOF; `close` отменяет поток. Подробный byte-credit/EOF
контракт определён в [движке](stream-engine.md).

`limits()` возвращает настроенный `ManagerConfig`, а `peer_limits(key)` —
`PeerLimits { receive_window, max_frame }` из проверенного OPEN или ACCEPT.
Для отсутствующего потока или устаревшего epoch/incarnation возвращается `None`.
Эти операции не меняют лимиты и не требуют I/O. Коннектор может отвергнуть
несовместимый профиль до выделения своих буферов.

При отсутствии работы `turn` ждёт готовности существующих подписок NATS и
ближайшего transport timer. Пришедшее сообщение обрабатывается до возврата
из ожидания. `turn_with_wake` также принимает `Future<Output = ()>` от host:
он опрашивается только в idle-ветке после завершения передачи извлечённых
output frames. Готовность host завершает это ожидание с нулевым transport
progress; обработка внешнего I/O остаётся обязанностью коннектора.
Host должен ожидать завершения всего turn. Внешний `select` с отменой активного
turn может потерять владение уже извлечёнными output frames и завершить транспорт.

`join_peer` идемпотентен для pending flight. Для уже ready peer прямой Core API
начинает новую negotiation: подтверждённая pair session replacement закрывает
старые streams. Host не должен считать повторный `join_peer` harmless connect.
IPC JOIN, напротив, обеспечивает готовность и является no-op для ready peer,
чтобы другой local owner не прерывал текущий обмен.

## Выдача прав и идентичность

Broker permissions должны связывать sender PeerId, recipient PeerId и shard.
Полученный subject без таких ACL не доказывает authenticated identity.
`Allowlist` дополнительно ограничивает принимаемые PeerId. `BrokerAuthorized`
разрешает новые broker-authorized identities в пределах `ManagerConfig.max_peers`,
включая pending joins. Это явный контракт доверия к provisioning брокера;
пересоздавать сервер для нового разрешённого клиента не требуется.

Темы NATS:

```text
<namespace>.join.<recipient>.<sender>
<namespace>.lane.<recipient>.<recipient-generation>.<shard>.data.<sender>.<sender-generation>
<namespace>.lane.<recipient>.<recipient-generation>.<shard>.control.<sender>.<sender-generation>
```

Все участники namespace используют один `shards` profile; shard вычисляется как
`sender PeerId % shards`. Например, при восьми shards, server PeerId0 и client
PeerId1 provisioner даёт клиенту publish только в `join.0.1` и
`lane.0.*.1.*.1.*`; server publish — в `join.*.0` и `lane.*.*.0.*.0.*`.
Subscribe разрешается только на собственный recipient, с необходимыми wildcard
для generations/shards/senders. Namespace prefix также ограничивается ACL.
У другого client нет права подменить sender1, читать recipient1 или публиковать
в чужой shard. Не выдавайте blanket publish/subscribe `>` и automatic reply
permissions, расширяющие эту границу. В NATS пустой allow-list не означает deny-all;
для роли без полномочий нужен явный `deny: [">"]`.

Broker/operator входит в доверенную границу: payload не зашифрован отдельно
между Core instances. Прикладной TLS внутри CONNECT сохраняет собственную проверку доверия.
Скомпрометированная/дублированная credential той же identity может заменить её
session; runtime не заменяет credential revocation.

## Присоединение, замена и готовность

Control v1 — ровно 77 bytes: `SKC1`, kind u8, sender generation u128, recipient
generation u128, initiator nonce u128, pair token/challenge u128, watermark u64;
числа big-endian. Это отдельный экспериментальный envelope; stream wire v2 и
его [fixtures](../core/tests/fixtures/README.md) сохраняются.

Node generation, initiator nonce и responder challenge генерируются OS randomness.
HELLO инициирует CHALLENGE; CONFIRM связывает обе generations и оба challenges;
READY завершает negotiation. Responder challenge становится pair-session token.
Повтор текущей flight идемпотентен, pending handshake имеет finite deadline.
Если оба peer одновременно начинают join, выигрывает инициатор с меньшим PeerId.
Старый HELLO может вызвать только новый bounded challenge, но не смену активной
session. Старый CONFIRM/READY/PONG или DATA не проходят текущие bindings.

При подтверждённой замене runtime аварийно завершает старые streams, выдаёт
terminal events и ждёт их drain. Затем снимает старую Manager registration,
создаёт новую pair session и новый local incarnation. Сохранённый `RuntimeKey`
содержит `(node epoch, peer incarnation, StreamKey)`; старые send/consume/close
возвращают `StaleKey`, даже если новый wire stream ID совпал. Incarnation и frame
sequence проверяются на overflow. Ранее выданные buffers остаются у host.

`async-nats::Client.flush()` завершает локальную запись socket buffers; это не
broker PONG и не доказательство установленной subscription. После negotiation
peer остаётся неготовым до matching nonce PONG на новой lane. DATA SUB создаётся
до CONTROL SUB на той же connection; успешный lane roundtrip подтверждает путь.
Первый потерянный warmup PING/PONG повторяется без продления исходного deadline.
`peer_ready` и `open` требуют этого доказательства.

Доказательство готовности локально: одна сторона может уже видеть ready,
пока другая ещё ждёт своего PONG. Полученный authenticated OPEN и действующий
ключ в этот момент не означают потерю поколения. Коннектор удерживает
согласование в пределах своего deadline и проверяет собственный `peer_ready`
перед активацией внешнего I/O.

```mermaid
sequenceDiagram
    participant A as Runtime A
    participant N as TLS NATS
    participant B as Runtime B
    A->>N: HELLO generation A, nonce A
    N->>B: Authenticated sender A
    B->>N: CHALLENGE generation B, nonce A, token B
    N->>A: CHALLENGE
    A->>N: Install DATA and CONTROL subscriptions
    A->>N: CONFIRM both generations and nonces
    N->>B: CONFIRM
    B->>B: Drain old streams, install lane, register pair
    B->>N: READY
    N->>A: READY
    A->>N: Lane PING nonce, dispatched watermark
    N->>B: Lane PING
    B->>N: Matching lane PONG after input applied
    N->>A: Matching lane PONG
    Note over A,B: Peer ready only after its own matching lane proof
    A->>N: First OPEN / DATA
    N->>B: OPEN / DATA
```

Host обязан регулярно вызывать `turn` и `poll_events`. Если terminal events не
дренированы за `terminal_drain_timeout`, runtime публикует
`TerminalDrainTimeout`, прекращает replacement flights и сохраняет ограниченную
старую terminal registration до drain. Он не выбрасывает уведомления и не
накапливает поколения. При global recovery такой stall переводит lifecycle в
`Failed`; при peer replacement другие peers могут продолжать работу.

## Живость и восстановление

DATA envelope содержит pair token u128 и последовательный frame sequence u64.
Разрыв sequence завершает peer. PING содержит nonce и число dispatched frames;
PONG разрешён только после применения всех входящих frames до watermark.
Ожидание DATA при опережающем CONTROL занимает один coalesced pending slot.
Это подтверждение применения входа, не потребления buffers коннектором.
Потерянный последний frame не маскируется живым брокером или lost overflow callback:
его watermark не подтверждается. Slow reader, который продолжает driving Core,
и полностью idle peer отвечают на heartbeat независимо от выдачи byte credit.

Missing PONG завершает peer после configured deadline. Наблюдаемый bound включает
heartbeat interval, peer timeout, bounded peer visitation и host driving/I/O delay.
Runtime не исполняется сам без host turns. Peer slots обслуживаются bounded
rotation; driver не сканирует все idle streams. Manager сохраняет indexed opening
deadlines. Heartbeats повторяют исходный nonce/watermark до исходного deadline.

Global join-transport loss переводит `Ready -> Recovering`: старый Manager
остаётся terminal, events дренируются, затем создаются новый Manager и generation.
Retries используют bounded exponential delay+jitter; после `max_retries` — `Failed`.
Authentication/authorization failure terminal; host должен исправить provisioning
и создать новое подключение. Брокер может быть ready при ещё неготовом peer:
проверяйте `peer_ready`, а не только lifecycle. Не возобновляются старые bytes.

`terminate_peer` завершает локальную session. `revoke_peer` запрещает будущий join
только для `Allowlist`; с `BrokerAuthorized` возвращает `Config`, потому что
broker credential revocation принадлежит provisioner. Ошибки разделены на Config,
Tls, Authentication/Authorization, Timeout, Admission, PeerUnavailable, StaleKey,
Transport/Protocol и безопасные Manager errors.

## Очереди, изоляция и состояние

Одна join connection и до `shards` lazy lane connections, по DATA и CONTROL
subscription на lane. Default shards8, допустимо1..32; отсутствуют connection/task
на каждый stream. Клиент к server PeerId0 использует только shard0, сервер —
не больше eight lanes данного профиля. CONTROL имеет собственную queue, join —
отдельный входной budget. Incoming shards вращаются после каждого message.

SlowConsumer/ошибка lane завершает peers этой shard. Другие shards сохраняют
прогресс; isolation внутри одной shard не обещается. Контрольный flood,
credential-authorized unlimited publish или broker-global saturation требуют
операционных rate/account limits. Broker `max_pending`/`write_deadline` ограничивают
его очереди. Повышение queue capacity не заменяет loss detection и admission.

Перед извлечением frames из Manager runtime ограничивает неподтверждённую
доставку отдельно от credit фактического потребления. Для одной session DATA
занимает не более 3 МиБ с учётом envelope и консервативного NATS framing;
управляющим frames доступен дополнительный headroom внутри 4 МиБ. Из этого
общего предела резервируется минимум 128 КиБ для retry/heartbeat controls;
длительные или частые retries увеличивают резерв, а небезопасный профиль
отклоняется при запуске. Это соответствует broker `max_pending: 4194304`;
меньший broker limit требует согласованного transport contract.

При заполнении половины DATA flight runtime отправляет lane PING. Matching
PONG освобождает только подтверждённый префикс отправленных bytes; более поздние
публикации остаются учтёнными. Broker flush сам по себе не освобождает этот
flight. Закрытый gate оставляет DATA в Core и допускает доступные управляющие
frames и работу других peers. Подтверждение доставки не возвращает receive
credit: для него по-прежнему необходимо фактическое потребление host.

Этот предел относится к одной producer→recipient session. Сумма публикаций
независимых клиентов в общую серверную lane не получает общей квоты этим
механизмом; при её перегрузке действуют описанные выше обнаружение потери и
изоляция shard. Ограничение памяти flight не является лимитом байтов в секунду.

DATA output публикуется bounded batch и flush выполняется на затронутые
connections. Cancellation guard действует для всех extracted frames до завершения
всех batch flushes: отмена может консервативно завершить все touched shards.
Во время незавершённой публикации/flush тот же owner обрабатывает established
lane input без новой очереди/task; immutable envelope сохраняет исходные token
и sequence. Поэтому общий incoming count turn может превышать обычный pass
budget, оставаясь ограниченным output batch и I/O deadline. Initial peer grants
не извлекаются до matching PONG readiness. Manager freeze failure также
завершает runtime session, даже если следующего DATA нет.
Retryable heartbeat/control batches также имеют отдельные конечные slots.

`RuntimeConfig::new` задаёт capacities128 DATA/CONTROL,128 join,16 commands,
64 inbound messages на каждый DATA/CONTROL pass и32 output frames/turn.
Control work дополнительно посещает до32 peers/turn с максимум четырьмя flights/
heartbeat messages на посещение; join input имеет отдельный budget16. Capacities
допускаются1..65536, batches1..256, retry attempts1..1024; durations максимум24h.
Manager limits задаются отдельно, включая max peers и global/per-peer streams/
receive/send budgets. Metadata limit512 bytes подходит для bounded destination
profile с255-byte hostname; parser в Core не входит.

`status()` использует O(1) Manager aggregate и counters, показывает active peers,
membership slots (включая pending), connections/retries/last error. `peer_status`
даёт явную per-peer диагностику; token/nonce не являются authentication secrets.
`resources()` остаётся O(stream count) диагностикой buffer occupancy.
Transport counters также включают число успешных DATA publish commands,
payload bytes, пять групп размеров DATA (`<1500`, `1500…8191`, `8192…16383`,
`16384`, `>16384`), WINDOW publish commands и завершённых socket flushes.
`output_elapsed_ns` измеряет суммарное wall time output batches, включая
ожидание publish/flush. Счётчики насыщаются на `u64::MAX`; они не измеряют CPU
и не подтверждают доставку или потребление payload удалённым приложением.
`validate_profile` учитывает queued messages и четыре дополнительных слота на
соединение: одну pending decoded Message, парсер и одну временную Message.
Обеспеченная граница —
`(join_capacity + 2*shards*subscription_capacity + (shards+1)*client_capacity + 4*(shards+1)) * (65588+1024) + max_peers*64`.
Последнее слагаемое обеспечивает состояние delivery flight и snapshot bytes
незавершённого PING каждого peer.
Парсер ограничен `2*(65588+516)` байтами capacity; тело каждого queued payload
имеет собственный буфер точной длины. Fixed body maximum 65588 действует до
первого INFO, при TLS upgrade и reconnect, независимо от broker max_payload.
INFO ограничен 4096 байтами, остальные control/MSG строки — 512. HMSG не
поддерживается: заголовки отклоняются до расширения HeaderMap и завершают
затронутое соединение. Текущий Runtime публикует plain PUB/MSG.

[Локальная поправка async-nats](../vendor/async-nats/PATCHES.md) сохраняет одну
pending Message при полной subscription, прекращая новые чтения, пока запись,
flush, commands и heartbeat продолжают работу. После io_timeout соединение
терминально закрывается. Это предотвращает silent DATA drop; произвольная пауза
host не обещает бесконечного ожидания. Upstream default и независимый static
NatsNode сохраняют исходный loss contract.

Это конечное userspace backing, не полный RSS: TLS, kernel TCP buffers, broker
queues и caller-owned allocations оцениваются отдельно.

## Воспроизводимая квалификация

[Первый запуск](getting-started.md#независимые-процессы-runtime) описывает `check`
и `qualify`: реальные TLS/auth/ACL, restart/rejoin, silent peer, tiny-queue overflow,
replay/stale keys, credential revocation и native trust. Test-only
`fault-injection` включён только `skvoz-testbench/real-nats`; обычный `nats` и
release `qualify` его не включают. Lost final DATA и cancelled extraction tests
явно inject failure; они не доказывают реальную потерю async-nats callback.

Qualification запускает отдельный release server и client processes, измеряет
server/client applications и broker отдельно. Delay forwarder задерживает реальные
TLS bytes, но также pacing8KiB chunks; это не полная эмуляция WAN. Hold после
active transfer проверяет bounded idle/liveness soak, не непрерывный traffic soak.
Local runs не устанавливают users/throughput/latency, mobile или whole-host SLA.


## Встраивание в сетевой runtime

[Общий TCP/IP runtime](network-runtime.md) встраивает один NatsRuntime и
единолично управляет событиями Core. Host на Ruby/GTK передаёт только команды;
обычные sockets/TUN и кредит после фактической записи обслуживает Rust.
Самостоятельный daemon/IPC остаётся независимым компонентом.

Публичный `verified_tls_config(&Trust, identity)` строит тот же проверяемый
ClientConfig для host, которому нужен dial на заранее подтверждённый numeric IP,
например enrollment при активном capture. Identity остаётся исходным DNS/IP SAN;
проверка цепочки и подписей TLS сохраняется. Host отвечает за фиксированный
адрес и lifecycle; этот builder не добавляет маршрутизацию в Core.
