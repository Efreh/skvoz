# Linux helper общего сетевого компонента

`skvoz-network-helper` 0.3.0 — узкий привилегированный Linux-процесс для
[общего сетевого модуля](../README.md). Он управляет только собственными TUN,
маршрутами, таблицей nft, per-link DNS и постоянными адресными назначениями.
Payload, NATS credentials и Core остаются вне helper. Весь Rust-код этого
пакета запрещает `unsafe`; системные вызовы находятся в [native](../native/README.md).

Исходники содержат server/client lifecycle и исполняющий процесс. Обычные тесты
проверяют schema, lease persistence и переходы через заменяемый Kernel; они
не устанавливают работоспособность nft, маршрутов, polkit, systemd или TUN в
конкретной системе. Перед использованием требуется квалификация целевого
окружения. Совместный [стенд runtime/helper](../../testbench/README.md)
проверяет kernel operations отдельно в одноразовых контейнерах.

## Запуск и привилегии

Сервер получает один connected private AF_UNIX канал:

```sh
skvoz-network-helper --config <root-policy.json> --control-fd <fd>
```

Конфигурация — строгий `HelperConfig` v1 из `skvoz_network::config`: `role=server`,
`state_dir` и `{network,server}` в `policy`. Она принадлежит root, имеет mode
0600 и расположена под root-owned nonwritable ancestors. PREPARE_SERVER должен
соответствовать этой static policy. У helper bounding/permitted/effective
содержат только NET_ADMIN, inheritable/ambient пусты; root bootstrap ограничивает
bounding set до запуска. Runtime — единственный owner канала. `SO_PEERCRED`
inherited socketpair отражает root creator, а не последующий UID runtime.
EOF закрывает live access и вызывает ограниченный cleanup; неудача не считается
успешным завершением. STOP_SERVER передаётся через runtime, без root signal от
непривилегированного host.

Helper проверяет, что forwarding уже включён в namespace. Для выбранных
семейств fragment high threshold должен быть не выше 4 MiB и timeout — не выше
15 s. Доступные conntrack IPv6 fragment settings проверяются отдельно. Значения
настраивает namespace bootstrap; helper не меняет host sysctl и не сбрасывает
другие настройки через переключение `ip_forward`. IPv6 требует явного routed
/64 и внешнего обратного маршрута; local preflight не доказывает upstream route.

На Ubuntu template socket принимает один owner соответствующего UID по адресу
`/run/skvoz-network-helper/<uid>/control.sock`: parents root-owned 0711, socket
UID-owned 0600. Unit-файлы находятся в `systemd/`, polkit action
`org.skvoz.network.manage` — в `polkit/`. `--listen-fd 3` принимает только один
systemd FD с правильными LISTEN_PID/FDS и endpoint. Приложение получает
interactive polkit authorization перед запуском сессии; helper повторно
проверяет connected PID/start ticks/UID без взаимодействия и не принимает
authority из JSON. Клиентская config имеет `policy:null`, `role:client` и
фиксированный `state_dir=/var/lib/skvoz-network-helper/<uid>`.

`--check-removal --config <root-policy.json>` проверяет только собственный
root-owned journal и exclusive lock, без создания файлов или kernel operations.
Guard, незавершённая операция, live state и повреждение вызывают ненулевой exit;
securely absent или полностью пустой ещё не инициализированный каталог допустимы.
Установщик проверяет состояние до и после отключения socket activation, чтобы
новый owner не появился между проверкой и удалением пакета. Удаление состояния
администратором при сохранённом kernel guard требует отдельного исправления.

Установщик создаёт root-owned config для выбранных UID и включает их socket
instances. Для пользователя, созданного позже, администратор должен подготовить
такую же config и включить его instance; template не выдаёт обычному пользователю
право запускать произвольный systemd unit. Runtime/UI остаются непривилегированными.

## Владение и восстановление

Server lease registry v1 ограничен 4096 identities и 8 grants на identity,
8 MiB сериализованного состояния. Grants постоянно закреплены за PeerId;
tombstone не освобождает адрес для другого peer. Потерянный marker/registry,
повреждение, overlap или изменение пула вызывают отказ. Новый grant публикуется
в памяти только после атомарной записи и fsync. State directory root-owned 0700
с отдельным nonwritable parent и exclusive lock; app-owned state не подходит.

Ownership journal v2 сохраняет namespace device/inode, собственный random token,
interface alias, отдельную route table, client configuration,
точные underlay interface/gateway snapshots и peer/session/grants.
Маршруты имеют protocol 186 и token-derived metric, lookup rules — priority 19760,
перед обычной main rule. Recovery проверяет значения и ownership tags, затем
удаляет только собственную table/rules/interface и conntrack по grants. При
неожиданном чужом объекте helper не удаляет его и возвращает ошибку. После смены
namespace новый journal создаётся только при отсутствии всех прежних объектов;
lease registry сохраняется, предыдущие namespace settings не применяются.

RESERVE_PEER записывает grant до CONFIG, ACTIVATE_PEER открывает access после
установки маршрутов, RETIRE_PEER сначала отзывает source access. Client
PREPARE_CLIENT сохраняет transport snapshot и создаёт guard до capture;
ACTIVATE_CLIENT разрешает только выбранные семейства через собственный TUN.
Transport exceptions разрешают точные broker IP/TCP ports, дополнительно —
loopback, DHCP и NDP. per-link DNS на TUN использует route-only domain `~.`;
блокировка прямого DNS выхода обеспечивается guard, поскольку более длинные
DNS domains других links могут иметь приоритет.

ABORT_CLIENT/owner EOF очищают TUN/routes/DNS и сохраняют guard. RESTORE_CLIENT
удаляет guard после cleanup только для известного handle и явного
`user_stop|mode_change|shutdown`. RECOVER возвращает `{state,handle}`: guarded
client сохраняет handle для явного восстановления после перезапуска приложения;
idle/server возвращают null. Новый authorized owner может подготовить новую
сессию после очистки предыдущих live resources.

## Проверка исходников

Из корня репозитория:

```sh
cargo test -p skvoz-network-helper --locked --offline
cargo clippy -p skvoz-network-helper --all-targets --locked --offline -- -D warnings
cargo run -p skvoz-network-helper --locked --offline -- --version
```

Обычные проверки не меняют kernel state. Совместный изолированный стенд
проверяет реальные capabilities, crash recovery, TUN/nft/routes,
NAT44/routed IPv6, resolved и polkit:

```sh
python3 testbench/run.py network-runtime
```

Этот режим не проверяет overflow всех kernel tables, DHCP/NDP при смене uplink,
интерактивное разрешение desktop или socket activation под systemd PID 1.

Канонический контракт runtime/helper: [локальный сетевой API](../../docs/network-runtime.md).
