# Контейнерный клиент

`skvoz-client` 1.0.0 — headless Linux/amd64 клиент на Ruby 4.0.7, использующий
тот же Rust Core/network, что Ubuntu, Android и сервер. Он подключается по обычным
адресу `DNS:port`, `IP:port` или `[IPv6]:port`, логину и паролю пользователя.
Полный сервер может быть доступен напрямую или через поддерживаемый стандартный
NATS-вход. Отдельная настройка топологии на клиенте не нужна.

## Быстрый запуск

Сохраните [Compose-пример](../clients/container/compose.yaml), замените `server`,
`username`, `password` внутри `SKVOZ_CLIENT_CONFIG` и выполните:

```sh
docker compose up -d
docker compose logs client
```

Образ — `ghcr.io/efreh/skvoz/skvoz-client:latest`. Для локальной сборки из корня
репозитория с Docker BuildKit:

```sh
docker build -f clients/container/Dockerfile --target deploy -t skvoz-client:local .
```

После этого укажите `image: skvoz-client:local` в Compose. CLI имеет команды
`serve`, `check`, `health`, `--version`. `check` проверяет YAML без подключения;
`health` возвращает JSON и код 0 только для текущего живого готового процесса.

Пример публикует proxy-порты только на loopback Docker-хоста. Для соседних
контейнеров используйте `http://client:8080`, `socks5h://client:1080`. Числовой
listener выбирает фактический private IPv4 контейнера; это не wildcard bind.
Явный listener имеет вид `127.0.0.1:8080` или `172.20.0.2:8080`; разрешены только
числовые loopback/private адреса. Не публикуйте неаутентифицированные входы
клиента в недоверенную сеть.

## Один non-VPN режим, несколько входов

В тот же inline YAML можно добавить именованные маршруты:

```yaml
server: server.example:4222
username: example
password: "your-issued-password"
mode: proxy
proxy:
  http: 8080
  socks: 1080
tcp:
  database:
    listen: 15432
    target: database.example:5432
http:
  api:
    listen: 8090
    upstream: https://api.example/v1/
```

Прокси HTTP поддерживает явные HTTP-запросы и CONNECT; SOCKS5 — TCP CONNECT.
Фиксированный TCP-вход отправляет байты одному назначению, сохраняя двусторонний
обмен и TCP half-close. В этот же порт можно передавать собственные TLS,
WebSocket или другие TCP-протоколы. SOCKS5 UDP ASSOCIATE и reverse publishing
не реализованы. `proxy: false` отключает стандартные proxy-listeners, оставляя
именованные маршруты. Все listeners должны быть свободны: ошибка bind не даёт
частичную готовность.

HTTP gateway обслуживает HTTP/1.1 к фиксированному HTTP(S) upstream. Входящий
`/chat?q=1` при base URL `https://api.example/v1/` становится `/v1/chat?q=1`.
Сохраняются method, query, binary body, Authorization и end-to-end headers;
Host и TLS SNI соответствуют настроенному upstream. Hop-by-hop headers и поля,
названные Connection, удаляются. Сжатые payload остаются непрозрачными.
TLS всегда проверяет CA и имя, включая localhost и IP. `ca_file` у server или
HTTP route задаёт явный абсолютный PEM-путь, смонтированный read-only; иначе
используются системные CA. Отключения проверки TLS нет.

Gateway передаёт request/response тела частями, поддерживает chunked и SSE,
не собирает всё тело в память и применяет обратное давление медленного читателя.
При разрыве upstream запрос не повторяется; POST не воспроизводится. Redirect
возвращается вызывающему приложению. Upgrade/WebSocket, HTTP/2, gRPC semantics и
trailers в gateway не поддерживаются; используйте opaque TCP/CONNECT маршрут,
если эти протоколы нужны. Ошибка upstream до ответа даёт 502, после начала
streaming — разрыв. Отмена потребителя закрывает связанную передачу.

## VPN для выбранных контейнеров

В том же Compose-файле замените конфигурацию `client` и добавьте потребителей:

```yaml
services:
  client:
    image: ghcr.io/efreh/skvoz/skvoz-client:latest
    restart: unless-stopped
    environment:
      SKVOZ_CLIENT_CONFIG: |
        server: server.example:4222
        username: example
        password: "your-issued-password"
        mode: vpn
        families: [4, 6]
    volumes:
      - client-state:/var/lib/skvoz-client
    tmpfs:
      - /run/skvoz-client:uid=10001,gid=10001,mode=0700,size=1m
    cap_drop: [ALL]
    cap_add: [NET_ADMIN, SETUID, SETGID, SETPCAP]
    devices: [/dev/net/tun:/dev/net/tun]
    security_opt: [no-new-privileges:true]
    cpus: 1
    mem_limit: 512m
    memswap_limit: 512m
    pids_limit: 64
    stop_grace_period: 10s
    ulimits: {core: 0, nofile: 8192}
  application:
    image: your-application:tag
    network_mode: service:client
    depends_on:
      client: {condition: service_healthy}
volumes:
  client-state:
```

На Docker-хосте должен существовать `/dev/net/tun`. Не нужны Docker socket,
`privileged`, host networking, маршруты Docker-хоста или desktop helper.
VPN и proxy/tcp/http конфигурации взаимоисключающие. `families` допускает `[4]`,
`[6]`, `[4, 6]`; сервер согласует доступное подмножество. Семейство без grant
остаётся закрытым. Это shared-netns сценарий; отдельного маршрутизатора для
произвольных bridge-сетей в первой версии нет.

Root bootstrap запускает узкий OS worker с единственной NET_ADMIN, затем Ruby
host и runtime работают UID/GID 10001 с пустыми capabilities и no-new-privileges.
Worker принимает только проверенный CONFIG, управляет своим TUN, nft table и
policy routing в namespace клиента. Чужой TUN/table/rule не удаляется. Второй
VPN owner в том же контейнере клиента отвергается до изменения namespace.
Потребители работают без NET_ADMIN; второй привилегированный клиент в общем
namespace не входит в поддерживаемый сценарий.

Guard ставится до healthy. Разрешены loopback, необходимый IPv6 NDP, один
числовой TLS endpoint и согласованные семейства через TUN. Docker DNS
`127.0.0.11` блокируется перед loopback. Адреса DNS из CONFIG записываются
**in-place** в общий Docker-generated `/etc/resolv.conf`; обычные DNS-пакеты
идут через TUN. `dns:` у контейнера с `network_mode: service:client` не задаётся:
Docker не поддерживает этот флаг. Потребитель видит тот же resolver file.
Клиенту нужен writable rootfs: read-only client rootfs не поддерживается этим
способом генерации resolver. Файлы кода остаются root-owned; потребитель может
иметь read-only rootfs.

Потеря runtime, транспорта, OS worker или остановка не открывает прямой выход.
Guard сохраняется, пока namespace удерживается потребителем. Новая попытка
создаёт новую сессию; открытые TCP-запросы и IP/NAT соединения не возобновляются.
Runtime loss восстанавливается внутри host; OS-worker loss завершает контейнер,
чтобы restart policy выполнила root bootstrap. Неверные credentials, TLS или
версии — terminal failure; автоматический рестарт не меняет учётные данные.

Endpoint закрепляется за числовым IP. Для повторного запуска в том же guarded
namespace root использует cache в `client-state/boot/`: старый tunneled resolver
может быть недоступен до восстановления TUN. Cache не отменяет проверку исходного
TLS имени. В новом namespace адрес разрешается заново. При смене server address,
DNS адреса сервера или пересоздании клиента пересоздайте также зависимые контейнеры:

```sh
docker compose up -d --force-recreate client application
```

После cold старта хоста/группы запускайте потребителей через health-gated Compose.
Автономный Engine restart после перезагрузки не гарантирует порядок запуска:
`depends_on` проверяет первоначальный Compose start, но не является постоянным
межконтейнерным guard. Не назначайте потребителю независимый auto-start, который
обходит это условие в новом namespace.

Старый работающий потребитель может удерживать старый namespace после пересоздания
клиента; автоматического переноса в новый namespace нет. `depends_on` управляет
стартом, но не мигрирует уже запущенный контейнер. Volume содержит один enrolled
device: не запускайте несколько клиентов с одним volume и не редактируйте его
файлы; для отдельного устройства используйте другой volume. Credential YAML и
volume приватны; исключите их из публикации и резервируйте с соответствующими правами.

## Ограничения и сборка

YAML ≤64KiB, strict keys/duplicates, до 32 именованных маршрутов, до 128 активных
Ruby connections, до 16 одновременно ожидающих OPEN_TCP/control operations;
все adapters используют одну enrolled identity и один runtime. Native proxy/TCP
также ограничен общим runtime budget. HTTP line ≤8192B, headers суммарно ≤64KiB,
body читается блоками ≤64KiB. Header deadline 10s, endpoint/TLS setup 15s,
blocked I/O progress 30s; idle SSE разрешён. Завершение ограничено 10s.
При сетевой ошибке новые попытки используют задержки 1,2,4,8,15s; старые запросы
не переигрываются. Клиент не обещает определённую скорость или transparent resume.

Обычный Dockerfile source-build компилирует общий native runtime из Cargo.lock,
устанавливает точный Ruby gem lock и не включает тестовые инструменты в deploy.
`tests` target запускает компонентные RSpec. Опциональный `artifact` target
упаковывает ранее собранные **для совместимого Ruby/Linux ABI** native runtime
и production bundle из `clients/container/artifact/`. Его `native-source.json`
проверяет SHA256 runtime, все source inputs, Gemfile/lock и bundle files; нельзя
подменять этот target произвольным старым бинарником. Default build остаётся
source-build; artifact packaging не доказывает повторную cold сборку.
