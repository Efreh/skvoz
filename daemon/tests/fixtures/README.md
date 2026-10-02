# IPC v1 vectors

`ipc-v1.tsv` содержит имя и hex полного фрейма, включая outer u32 length.
HELLO, OPEN, binary SEND, CONSUME и CLOSED соответствуют
[каноническому контракту](../../../docs/daemon-ipc.md). Integer fields big-endian;
handle использует128bits, DATA/metadata остаются raw bytes. Rust tests читают
эти public fixtures; Python/Ruby encode позволяют независимо сверить формат.
Это deterministic vectors, не credentials или captured runtime data.
