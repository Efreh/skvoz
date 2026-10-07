# NATS packet v2

Core 4.0.0 использует wire 2; другие версии отвергаются. Одна публикация
содержит один packet без padding. Все целые unsigned, big-endian. Максимум
65 564 байта. Header: `SKVZ` (4 байта), version=2 (u8), kind (u8),
reserved=0 (u16), stream_id (u64).

Для потока ID = `(монотонный sequence << 1) | origin_bit`, sequence начинается
с 1. В NatsNode меньший PeerId использует origin_bit=0, больший — 1; Manager
получает эту настройку от host. Идентичность потока — `(PeerId, stream_id)`.
ID 0 зарезервирован исключительно для peer-control kinds 9–12.

| kind | Фрейм | Body |
| --- | --- | --- |
| 1 | OPEN | receive_window u32, max_frame u32, metadata_len u32, metadata |
| 2 | ACCEPT | То же |
| 3 | REJECT | reason_len u32, opaque reason |
| 4 | DATA | offset u64, len u32, непустые bytes |
| 5 | WINDOW_UPDATE | consumed u64 |
| 6 | FIN | final_offset u64 |
| 7 | CLOSE | reason u8: 1 Cancelled, 2 TransportLost, 3 ProtocolError, 4 OpenTimeout |
| 8 | WINDOW_GRANT | consumed u64, limit u64, probe u64 |
| 9 | PEER_GRANT | epoch u64, consumed_bytes u64, limit_bytes u64, consumed_records u64, limit_records u64, probe u64 |
| 10 | PEER_REQUEST | bytes u32, records u32, probe u64, requester_stream_id u64, blocked u8 |
| 11 | PEER_FREEZE | epoch u64 |
| 12 | PEER_FROZEN | epoch u64, dispatched_bytes u64, dispatched_records u64 |

Окно и max_frame проверяются до копирования metadata. DATA ≤65 536 байт,
metadata/reason ≤65 536 байт; конкретные профили задают меньшие пределы.
Неверная длина, trailing bytes, unknown kind/version/reserved, неправильный
stream ID и oversized packet отвергаются. Новые OPEN упорядочены;
монотонный high-water не позволяет повторно открыть закрытый ID.

## Кредит и перераспределение

WINDOW_UPDATE подтверждает только фактическое потребление непрерывного
префикса направления. WINDOW_GRANT отдельно увеличивает абсолютную границу
отправки `limit`; `consumed` остаётся подтверждением потребления, а `probe`
возвращает запрос измерения времени. Отправитель сохраняет конечные offsets
DATA до подтверждения; record allowance потока определяется одинаково на
обеих сторонах: `min(receive_window / max_frame + 64, 65536)`.

Manager дополнительно ограничивает DATA суммарным peer-кредитом в байтах и
записях. PEER_GRANT содержит накопительные границы: pending send резервируется
до извлечения, DATA в пути и у получателя остаются долгом до actual consumption.
PEER_REQUEST сообщает требуемое окно, probe и ID живого потока, которому
нужен ресурс; это запрос, а не разрешение. `blocked` — битовая маска фактически
исчерпанного aggregate credit: 1 для bytes, 2 для records, 3 для обоих,
0 для запроса роста. Исторический закрытый ID безопасно игнорируется;
неизвестный будущий ID отвергается по sequence/origin текущего peer.
Начальный кредит каждого зарегистрированного peer обеспечен общим бюджетом.
Неиспользованное уже объявленное разрешение тоже занимает backing.

Окно потока растёт контролируемым удвоением по его собственному actual
consumption и RTT. Локальная отправка оставляет 1/8 обеспеченного peer flight
в байтах и records для прогресса других потоков; pending и dispatched DATA
до собственного actual ACK одинаково занимают этот ресурс. Это ограничение
удерживаемого backing, а не bytes/s или квота по числу потоков.

Несколько непрочитанных потоков могут совместно занять конечный пул. При
реальном aggregate deficit для другого живого requester Manager может
завершить получивший DATA поток без собственного consumption в течение
`max(1000 мс, 4 × RTT)`. Выбирается наибольший непотреблённый объём в
исчерпанном измерении; requester и продолжающие потреблять потоки исключены.
Используется CLOSE(1, Cancelled): этот код обозначает также отмену из-за
дефицита ресурсов, а не только действие пользователя. Собственный запрос
одиночного paused stream не является основанием такой отмены. Переключение
requester не переносит уже начавшийся deadline.
Requester должен ещё принимать DATA направления (`Open` или
`HalfClosedLocal`); полученный FIN снимает эту потребность, поэтому прежний
запрос не может отменить соседний непрочитанный поток после FIN.

PEER_FREEZE останавливает новые send одного направления. Ранее принятые DATA
передаются в том же аутентифицированном упорядоченном канале **перед**
PEER_FROZEN. Его totals должны совпасть с фактически принятыми получателем.
Только после этой границы получатель может уменьшить неиспользованное
разрешение, выдать PEER_GRANT нового epoch и передать освободившийся backing
другому peer. Уже пришедшие непотреблённые данные сохраняют обеспечение.
Направления независимы; control не требует DATA-кредита. Ошибка totals,
переполнение счётчиков или deadline завершают затронутую peer session.

Неизвестный или закрытый stream не позволяет отменить обещанный кредит:
пришедший DATA сначала учитывается peer-ledger, затем осознанно отбрасывается
и только эти пришедшие bytes/record считаются потреблёнными. Duplicate/gap,
устаревший или нарушающий разрешение DATA завершают peer. Старая authenticated
session не может использовать кредит нового поколения.

## Транспорт

Static NatsNode использует subject
`<namespace>.<recipient>.<recipient-session>.<sender>.<sender-session>` и
subscription `<namespace>.<local-id>.<local-session>.*.*`. Host задаёт routes
и новую session generation; ACL связывает sender с credentials. Wire packet
самостоятельно не доказывает identity.

[Динамический NatsRuntime](nats-runtime.md) добавляет pair token и frame
sequence поверх того же wire 2. Peer-control и DATA сохраняют общий порядок;
первые grants извлекаются только после lane PONG readiness. Disconnect не
возобновляет прерванные bytes. Static routes требуют пересоздания NatsNode
при изменении session, без автоматического discovery.

Language-neutral hex vectors находятся в
[wire-v2.tsv](../core/tests/fixtures/wire-v2.tsv), формат — в
[README fixtures](../core/tests/fixtures/README.md).
