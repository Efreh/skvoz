# Stream fixtures

UTF-8 text; each nonempty non-comment line is `COMMAND [arguments] => expected`.
Payloads/metadata are lowercase hex; `-` is empty. A/B are independent streams
with window 8 bytes, frame size 4 bytes, 2 pending DATA frames, metadata limit
8 bytes and opening timeout 10 ms. Time starts at 0. TRANSFER drains frames in
order and immediately delivers them at time 0; TICK explicitly advances time.
EVENTS takes a batch count. Expected lists are comma-separated; `-` is empty.

Commands: OPEN, ACCEPT, REJECT, SEND, CONSUME (absolute byte offset), FINISH,
TRANSFER (from/to), EVENTS, STATE, TICK. Frames/events use the names below in
the checked files; numeric offsets/windows are decimal. SEND may return a
partial `accepted(n)` or `would_block`. Result `ok` means no local API error.

These scenarios are language-neutral behavioral fixtures. They do not encode
a network frame. Rust's [semantic_fixtures.rs](../semantic_fixtures.rs) executes every scenario;
another implementation can run the same commands and compare the same trace.

`wire-v1.tsv` contains seven language-neutral binary packet vectors. Each
non-comment line is `name<TAB>hex`; packets use stream ID 2 and payload/metadata
`00ff` where applicable. [wire_contract.rs](../wire_contract.rs) checks encoding
and decoding against these bytes. The experimental packet format is described
in [docs/wire.md](../../../docs/wire.md).
