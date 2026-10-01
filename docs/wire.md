# Экспериментальный NATS packet v1

Одна публикация содержит один packet, без padding. Все целые — unsigned,
big-endian. Максимум 65 564 байта. Нулевой stream ID запрещён. Это формат
тестового стенда, не стабильная публичная совместимость.

Header: `SKVZ` (4 bytes), version=1 (u8), kind (u8), reserved=0 (u16),
stream_id (u64). ID = (монотонный sequence << 1) | origin_bit; user=0,
consumer=1. Две фиксированные роли имеют один inbox subject каждая:
`skvoz.bench.<run-token>.<case-token>.user|consumer`.

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
создаёт новую сессию; origin/монотонность проверяет драйвер.

Decoder выполняет только bounded allocations. Полная проверка аутентификации
identity/discovery и гарантий reconnect требует отдельного production дизайна.

Language-neutral hex vectors находятся в
[core/tests/fixtures/wire-v1.tsv](../core/tests/fixtures/wire-v1.tsv);
их формат описан в [README](../core/tests/fixtures/README.md).
