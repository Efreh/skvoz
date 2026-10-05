# Нативная Linux boundary сетевого компонента

`skvoz-network-native` 0.1.0 содержит безопасные Rust wrappers над Linux
дескрипторами, TUN ioctl и `SCM_RIGHTS`. Пакет зависит только от `libc` и не
содержит сетевой state machine, Core, маршрутизацию, firewall или DNS.
Основные модули сохраняют запрет unsafe; этот узкий пакет проверяет каждую
операцию через собственную границу владения.

`TunDevice::create(name, mtu)` создаёт непостоянный одноочередный TUN в текущей
network namespace, запрещает присоединение к уже существующему интерфейсу,
выключает offloads и задаёт MTU 576…1500. Нужны `/dev/net/tun` и
`CAP_NET_ADMIN`. Адреса, состояние link и маршруты настраивает владелец namespace.
`from_owned_fd` принимает уже неблокирующий `OwnedFd`, проверяет тип/флаги TUN и
устанавливает `CLOEXEC`; при ошибке переданный FD закрывается. Формат пакетов —
`IFF_TUN|IFF_NO_PI`, без VNET/GSO; `IFF_NOFILTER` проверяется как служебный
результат `TUNGETIFF`. Дополнительные флаги отклоняются.

`try_read_packet` требует buffer не меньше MTU+1, чтобы обнаружить oversized
пакет. `try_write_packet` передаёт целый пакет одним вызовом; short write
возвращает ошибку, оставшаяся часть отдельным пакетом не отправляется. Оба метода
возвращают `WouldBlock` без ожидания. Асинхронный driver может использовать
`AsRawFd` с `AsyncFd`; он ограничивает pending write одной секундой и не
назначает stall deadline здоровому idle reader.

`duplicate_cloexec` и `duplicate_inherited` создают отдельный owned FD,
сохраняя исходное владение и режим open-file description. Проверка
`O_NONBLOCK` при admission не меняет совместно используемый режим caller.

`FramedUnix` принимает connected неблокирующий AF_UNIX SOCK_STREAM и читает
`u32be length + body`, где body содержит 1…32768 bytes. Один FD разрешён только
на первом байте frame. `MSG_CMSG_CLOEXEC`, truncated ancillary и лишние FD
проверяются; полученные копии закрываются при любом reject. Передача сохраняет
FD отправителя. JSON/schema/`fd_count` проверяет вызывающий protocol adapter.
`peer_credentials` возвращает `SO_PEERCRED`; решение о допустимом UID принимает
владелец API.

Idle `receive_frame` возвращает `WouldBlock`; после первого байта frame должен
закончиться за 5 секунд. `receive_frame_until` задаёт также начальный deadline
для handshake. Запись ограничена 3 секундами. Любой framing/I/O failure делает
channel terminal и выполняет shutdown. Это синхронные операции для отдельного
control thread; независимые reader/writer используют отдельные duplicates
того же socket. Packet I/O не проходит через control frames.

`IncrementalUnix` предназначен для асинхронного owner: `queue_frame` принимает
один owned frame, `try_flush` и `try_receive_frame` сохраняют частичные bytes/FD
при `WouldBlock`. `AsFd`/`AsRawFd` позволяют использовать `AsyncFd`. Reader и
writer имеют независимые состояния; idle read не имеет таймера. Owner должен
ожидать `next_deadline()` и вызывать `check_deadlines()` даже без новой readiness:
начатый read ограничен 5 секундами, queued write — 3 секундами. Fatal error
закрывает channel и pending FD. JSON/schema остаются обязанностью adapter.

Из корня репозитория:

```sh
cargo test -p skvoz-network-native --locked --offline
cargo clippy -p skvoz-network-native --all-targets --locked --offline -- -D warnings
```

Тест `linux_tun` запускается явно через `--ignored` только в одноразовом
контейнере/network namespace с `/dev/net/tun`, `CAP_NET_ADMIN` и `iproute2`.
Он проверяет настоящий TUN, владение FD, запись и удаление интерфейса.
Обычные тесты не изменяют маршруты или firewall основной машины.

`read_root_config` читает ограниченный root-owned файл 0600 через проверенные
root-owned каталоги без group/other write. `SecureStateDir` требует leaf 0700,
держит anchored FD и exclusive lock; файлы проверяются на тип, owner, mode и
единственную hard link. Замена проверяет существующую цель, создаёт новый файл
0600 с `O_EXCL`, выполняет file fsync, rename относительно FD и directory fsync.
Повторное перечисление открывает отдельный directory cursor. Symlink и
неожиданные объекты отклоняются.

`read_private_config` предназначен для непривилегированного runtime: файл
принадлежит текущему effective UID и имеет mode 0600, непосредственный каталог
— тому же UID и mode 0700. Ancestors принадлежат root или этому UID; единственное
исключение writable parent — root-owned sticky 01777, например временный каталог.
Разрешение этого исключения не распространяется на root config или lease state.

`inherit_control` настраивает child FD 3…63 через отдельную CLOEXEC-копию и
async-signal-safe `pre_exec`/`dup2`. Флаги parent FD не меняются; после spawn
нужно удалить `Command`, освобождая его временную копию. `adopt_inherited`
предназначен для явно переданного CLI FD: после успешного дублирования закрывает
его исходный номер. Borrowed FFI по-прежнему использует `duplicate_inherited`.
`restrict_helper_caps` проверяет bounding set с одним `CAP_NET_ADMIN`, очищает
inheritable/ambient и устанавливает ровно NET_ADMIN в permitted/effective.

`configure_socket_buffers` запрашивает не более 128 KiB на направление и
возвращает фактические значения `SO_SNDBUF`/`SO_RCVBUF`, включая kernel doubling.
Эти значения учитываются отдельно от userspace budget. `owner_closed` наблюдает
peer shutdown без чтения protocol bytes; `shutdown_write` выполняет half-close.
EOF/FD/readiness wrappers не принимают решения о сетевых session или правах API.

Канонический контракт runtime/helper: [локальный сетевой API](../../docs/network-runtime.md).
