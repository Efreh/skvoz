# Минимальный TCP relay

Экспериментальный Rust package `skvoz-tcp` в общем workspace. Владеет одним
TCP-сокетом и небольшими I/O буферами; использует ту же библиотеку
[Core](../../core/README.md), что и другие коннекторы. Маршрутизация и
flow control находятся в Core.

`relay_one(&mut NatsNode, StreamKey, TcpStream, Duration)` получает уже открытый
Core stream и выделенный для этого сокета NatsNode. Host самостоятельно
создаёт сокет, настраивает peer/session routes и принимает OPEN. Relay передаёт
binary bytes в обоих направлениях; TCP EOF вызывает FIN, а RemoteFinished —
shutdown отправляющего направления сокета. Ответ после EOF запроса допустим.

Relay хранит не более 1024 байт ожидающей отправки и один выданный Core DATA
chunk (не больше настроенного max_frame). Кредит возвращается только после
полной записи и освобождения chunk. Общий timeout охватывает и awaited I/O.
При ошибке или deadline relay освобождает буферы, отменяет Core stream и даёт
до 250 мс на передачу CANCEL/обработку terminal work, сохраняя исходную ошибку.
Это bounded попытка доставки, а не гарантия подтверждения отмены удалённым peer.
Если bounded попытка не завершилась, relay локально fail-close peer и выдаёт
terminal events, освобождая его reservation. Отмена output future навсегда
защёлкивает failure NatsNode. Remote silent loss требует отдельного liveness;
будущие перезапуски relay не возобновляют старый поток.

Воспроизводимый real TCP пример и failure checks запускаются из корня:

```sh
python3 testbench/run.py tcp
python3 testbench/run.py check
```

[Первый запуск](../../docs/getting-started.md) описывает зависимости и контейнер.
Happy path проверяет 32 768 байт запроса, FIN, затем 65 536 байт ответа и EOF.
Check также проверяет настоящий socket write error и общий deadline с получением
CANCEL через NATS и освобождением фактических ресурсов обоих nodes.

Это пример одного соединения, не законченный многопользовательский proxy:
нет listen/accept routing policy, выбора egress, SOCKS/HTTP CONNECT, multiplexed
socket dispatcher, reconnect/resumption или mobile integration. Для многих
сокетов нужен отдельный connector dispatcher поверх общего NatsNode.
