# Fixtures потока

UTF-8: каждая непустая строка без комментария имеет вид
`COMMAND [arguments] => expected`. Payload и metadata — lowercase hex,
`-` означает пустое значение. A/B — независимые потоки с окном 8 байт,
max_frame 4 байта, двумя pending DATA, metadata limit 8 байт и timeout 10 мс.
Время начинается с 0. TRANSFER извлекает фреймы по порядку и немедленно
передаёт другой стороне; TICK явно меняет время. EVENTS принимает размер batch.
Списки разделены запятыми, offsets/windows записаны десятичными числами.

Команды: OPEN, ACCEPT, REJECT, SEND, CONSUME (абсолютный offset), FINISH,
TRANSFER (from/to), EVENTS, STATE, TICK. SEND принимает не более одного DATA
frame за вызов и может вернуть `accepted(n)` или `would_block`.
`ok` означает отсутствие ошибки локального API.

[semantic_fixtures.rs](../semantic_fixtures.rs) исполняет эти независимые от
языка сценарии; другая реализация может сравнить тот же trace.

`wire-v2.tsv` содержит двенадцать бинарных векторов: `name<TAB>hex`.
Stream frames используют ID 2, peer-control — ID 0; payload/metadata — `00ff`.
[wire_contract.rs](../wire_contract.rs) сравнивает encoding/decoding с этими
байтами. Контракт формата — [wire](../../../docs/wire.md).
