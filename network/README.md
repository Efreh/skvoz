# Общий сетевой модуль

`skvoz-network` 0.4.0 содержит переносимые сетевые состояния и Linux runtime,
который использует один [Core/NatsRuntime](../docs/nats-runtime.md). Core
передаёт непрозрачные байты; сетевой модуль проверяет network v4, управляет
TCP-потоками и полными IPv4/IPv6-пакетами. Локальный контракт, строгая
конфигурация и команды описаны в [руководстве runtime](../docs/network-runtime.md).

`RuntimeHandle` и `skvoz-network-runtime` используют один actor. Локальный
HELLO не ждёт подключения NATS. Клиент выбирает взаимоисключающие режимы
`proxy` и `ip`; сервер принимает оба сетевых профиля через тот же Core.
HTTP/CONNECT/SOCKS5 CONNECT разбираются внутри Rust; payload не передаётся
через управляющий JSON. OPEN_TCP возвращает owned Unix FD после удалённого
ACCEPT. Остаток частичного SEND сохраняется, кредит принимаемых TCP-байтов
возвращается только после завершённой записи в локальный сокет.

IP-сессия согласует CONFIG, открывает все каналы и проходит READY/ACTIVE.
Пакеты проверяются по семейству, длине, MTU и source grants. Сессия привязана
к PeerId, epoch/incarnation Core и своему идентификатору. Серверный runtime
единолично владеет helper channel: RESERVE предшествует CONFIG, ACTIVATE
подтверждается до ACTIVE, RETIRE закрывает доступ прежней сессии. Linux
маршруты, firewall и постоянные назначения принадлежат отдельному helper.

`enqueue_packet` сохраняет остаток частичной отправки Core. `poll_packet`
передаёт пакет I/O с отметкой окончания записи, а `complete_packet` возвращает
кредит после полной записи либо осознанного отбрасывания. Извлечение или
копирование пакета не подтверждает потребление; позднее завершение не обходит
более ранний незавершённый пакет. Очереди и общий ledger ограничивают байты и
число записей. Kernel socket buffers и память системного resolver учитываются
отдельно при квалификации.

Граница Linux FD/TUN/SCM_RIGHTS находится в [native/](native/README.md),
привилегированные операции — в [helper/](helper/README.md), тонкая C ABI1 —
в [ffi/](ffi/README.md). Основная библиотека сохраняет запрет `unsafe`.
FFI не создаёт второй engine или собственную очередь событий. Недостаточный
буфер сохраняет ту же authoritative запись и FD до успешного получения.

Из корня репозитория:

```sh
cargo build -p skvoz-network --features linux-runtime --locked
cargo test -p skvoz-network --features linux-runtime --locked
cargo test -p skvoz-network-native --locked
```

Исполняемый файл получает private JSON и подключённый управляющий Unix FD:

```sh
skvoz-network-runtime --config private.json --control-fd 3
```

Для сервера с IP backend дополнительно обязателен `--helper-fd 4`.
Launcher передаёт назначенные FD исключительно дочернему процессу; обычные
числа чужих FD вместо SCM_RIGHTS не принимаются. `--help` и `--version`
работают без подключения NATS. STOP подтверждается после ограниченного
завершения собственных потоков; ошибка или deadline не выдаётся за успешную
очистку. Потеря владельца останавливает runtime, helper закрывает live доступ.

Детерминированные проверки покрывают кодеки, владение FD, частичные TCP-записи,
half-close, локальный HELLO и ограничения. Они не заменяют реальную проверку
NATS/TUN, kernel policy, установки Ubuntu, платформ или производительности.
Изолированный [стенд](../testbench/README.md) запускается отдельно:

```sh
python3 testbench/run.py network
python3 testbench/run.py network-runtime
```

Он требует Docker с `/dev/net/tun` и использует capabilities только в
одноразовых контейнерах. Поддержка Android и гарантии скорости этим компонентом
не объявляются.
