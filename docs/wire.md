# Экспериментальный NATS packet v1

Одна публикация содержит один packet, без padding. Все целые — unsigned,
big-endian. Максимум 65 564 байта. Нулевой stream ID запрещён. Это экспериментальный формат
Core, не стабильная публичная совместимость.

Header: `SKVZ` (4 bytes), version=1 (u8), kind (u8), reserved=0 (u16),
stream_id (u64). ID = (монотонный sequence << 1) | origin_bit, sequence начинается
с 1. В NatsNode peer с меньшим числовым PeerId использует origin_bit=0, с большим
— 1. Manager без NATS получает эту pair-local настройку от host. Тот же stream ID
может существовать у разных peers: идентичность потока — `(PeerId, stream_id)`.

Routing envelope вне binary packet:
`<namespace>.<recipient>.<recipient-session>.<sender>.<sender-session>`.
Одна subscription node — `<namespace>.<local-id>.<local-session>.*.*`.
Host задаёт routes и новую session generation на каждый lifetime. Драйвер
проверяет точное registered sender/session, ACL NATS связывает sender identity
с credentials и разрешёнными получателями. Пакет не несёт самостоятельного
proof identity; wildcard subscribe не заменяет ACL. Legacy fixed-pair demo
использует PeerId user=0 / consumer=1 поверх того же envelope.

| kind | Фрейм | Body |
| --- | --- | --- |
| 1 | OPEN | receive_window u32, max_frame u32, metadata_len u32, metadata |
| 2 | ACCEPT | То же |
| 3 | REJECT | reason_len u32, opaque reason |
| 4 | DATA | offset u64, len u32, bytes (непустые) |
| 5 | WINDOW_UPDATE | consumed u64 |
| 6 | FIN | final_offset u64 |
| 7 | CLOSE | reason u8: 1 Cancelled, 2 TransportLost, 3 ProtocolError, 4 OpenTimeout |

Окно и max_frame проверяются по абсолютным границам core до копирования
metadata. DATA <= 65 536 байт, metadata/reason <= 65 536 байт. Установленные
движком лимиты могут быть меньше. Несовпадение длины, trailing bytes, unknown
kind/version/reserved или oversized packet отвергаются. Повтор OPEN не
создаёт новую сессию; origin/монотонность проверяет Manager. Per-peer high-water
sequence не хранит неограниченный список закрытых IDs: старый OPEN не создаёт
entry снова. Host/transport обязан сохранять порядок новых OPEN одного peer.
Unknown non-OPEN packets игнорируются. При overload допускается только один
pending REJECT на peer; остальные OPEN могут завершиться remote timeout.

Decoder выполняет только bounded allocations после проверки wire limits. Более
узкие stream/Manager limits проверяются следующим слоем. У fixed-route NatsNode
static routes не обновляются автоматически: смена configured session требует
пересоздания node. Dynamic NatsRuntime поддерживает authenticated join, новые
generations и active peer liveness, как описано в [его контракте](nats-runtime.md).
Disconnect не возобновляет прерванные stream bytes ни в одном runtime.

Language-neutral hex vectors находятся в
[core/tests/fixtures/wire-v1.tsv](../core/tests/fixtures/wire-v1.tsv);
их формат описан в [README](../core/tests/fixtures/README.md).

Опциональный [динамический NATS runtime](nats-runtime.md) добавляет оболочку с токеном пары и номером последовательности, а также ограниченные управляющие пакеты поверх того же wire v1. Векторы потока остаются действительными; статический NatsNode передаёт исходный пакет без оболочки runtime.
