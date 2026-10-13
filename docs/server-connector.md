# Серверный коннектор

`skvoz-server` 7.0.0 — Ruby 4.0.7 host общего сетевого runtime: он управляет
NATS 2.15.1, пользователями, устройствами, Auth Callout и TLS.
TCP-сокеты и полные IP-пакеты обслуживает `skvoz-network-runtime` 0.6.0,
встраивающий Core 4.1.0 (network 6, API 1).
Привилегированные операции выхода выполняет узкий Linux helper.
Одна реализация сервера принимает совместимые клиентские коннекторы независимо
от их ОС и языка. [Общий контракт](network-runtime.md) определяет данные,
локальное управление и владение FD.

Исходники новой интеграции не означают завершённую квалификацию установленного
gateway. Сборка образа, реальные IPv4/IPv6 маршруты, capabilities, восстановление
и нагрузка проверяются в целевом окружении отдельно.

## Диагностика

```sh
sudo docker compose -p skvoz -f /etc/skvoz/compose.yaml exec -T --user 10001 server skvoz-server health
sudo docker compose -p skvoz -f /etc/skvoz/compose.yaml logs --timestamps --since 30m server
```

Host пишет ограниченные JSON-события в stderr с временем UTC, event, pid,
stage и безопасными кодами ошибок. Произвольный текст исключений, credentials,
payload и вывод дочерних процессов не публикуются. Health получает состояние
общего runtime и его агрегированные счётчики через STATUS. Ready означает
готовность транспорта и host, а не доступность любого назначения.
После смерти runtime/helper или потери текущего Core transport runtime
закрывает принадлежащие ему ресурсы, а серверный supervisor завершает контейнер.
Compose перезапускает его; клиент создаёт новую попытку подключения к peer `0`.

## Развёртывание через Compose

Нужны Linux x86_64, Docker и Docker Compose 2.24.4+. Для standalone или входной
ноды нужен доступный клиентам IP-адрес или доменное имя; выходная нода сама
подключается к входу и не требует публичного listener.
Требуемое поведение переопределений описано
в [правилах объединения Compose](https://docs.docker.com/reference/compose-file/merge/).
При автоматическом выпуске сертификата входа внешний TCP-порт 80 должен вести
к обработчику проверки HTTP01, а выбранный порт брокера — к NATS на этом сервере.
Центр сертификации ACME должен поддерживать
профиль `shortlived` и IP-идентификаторы при установке по IP-адресу. По умолчанию
используется рабочий каталог Let's Encrypt. Его
[профиль короткоживущих сертификатов](https://letsencrypt.org/docs/profiles/#shortlived)
поддерживает IP-адреса и доменные имена. Перед выпуском примите условия центра сертификации.

Из корня репозитория скопируйте шаблон в постоянный каталог установки:

```sh
sudo install -d -m 0700 /etc/skvoz
sudo install -m 0600 connectors/server/compose.yaml /etc/skvoz/compose.yaml
```

В копии `compose.yaml` укажите публичный адрес и email для сертификатов.
После принятия условий центра сертификации задайте
`SKVOZ_ACME_TERMS_AGREED: "true"`. Все параметры установки находятся в этом YAML.
Переменные в секции `environment` передаются внутрь контейнера самим Compose.
Пример изменяемой части файла:

```yaml
services:
  server:
    image: ghcr.io/efreh/skvoz/skvoz-server:latest
    environment:
      SKVOZ_ADVERTISED_PORT: &listen_port "4222"
      SKVOZ_ADDRESS: YOUR_PUBLIC_IP_OR_DNS_NAME
      SKVOZ_ACME_EMAIL: YOUR_CERTIFICATE_CONTACT_EMAIL
      SKVOZ_ACME_TERMS_AGREED: "true"
      SKVOZ_ALLOW: '[]'
```

Остальные поля сохраните из шаблона. Запустите готовый образ:

```sh
sudo docker compose -p skvoz -f /etc/skvoz/compose.yaml config --quiet
sudo docker compose -p skvoz -f /etc/skvoz/compose.yaml up -d
sudo docker compose -p skvoz -f /etc/skvoz/compose.yaml exec -T --user 10001 server skvoz-server health
```

По умолчанию используется `ghcr.io/efreh/skvoz/skvoz-server:latest`. Этот тег
обновляется после успешных проверок основной ветки и публикации версий.
При необходимости замените его тегом выбранной версии или неизменяемым digest
со страницы GitHub Packages этого репозитория. После успешных проверок процесс CI
публикует образ в `ghcr.io/<owner>/<repository>/skvoz-server`; наличие файла CI
само по себе не означает, что образ уже опубликован. Настройки видимости и доступа
пакета должны разрешать серверу загрузку. Digest фиксирует точное содержимое образа.

В образ входят Ruby, библиотеки с зафиксированными версиями, общий runtime,
helper и брокер. Том `state` хранит пользователей, назначенные идентификаторы,
TLS и учётную запись ACME. Отдельный root-owned том `network-state` хранит
постоянные сетевые назначения и журнал helper; не объединяйте эти каталоги.
Сохраняйте оба тома при обновлении. Копия `state` содержит пароли и закрытые ключи.

Контейнер начинает с root bootstrap и только NET_ADMIN/SETUID/SETGID/SETPCAP.
Bootstrap запускает helper с NET_ADMIN, затем exec переводит Ruby PID 1 в UID
10001 без capabilities. NATS и runtime также непривилегированны. Включены
read-only filesystem, no-new-privileges и ограниченный tmpfs; TUN доступен
через `/dev/net/tun`. Наличие таких настроек требует фактической проверки
capability sets после запуска. Init-процесс перед Ruby не используется.

Значение `SKVOZ_ADVERTISED_PORT` с YAML-якорем `&listen_port` по умолчанию
равно `"4222"`. Оно задаёт внешний порт в профилях клиентов и через `*listen_port`
в секции `ports` — публикацию брокера на машине. Меняйте значение якоря в одном
месте; внутри контейнера брокер продолжает слушать 4222. Внешний порт 80
направляется на внутренний 8080. `SKVOZ_ALLOW` задаёт JSON правил доступа; по умолчанию разрешены публичные
unicast назначения. Без `SKVOZ_NETWORK` сервер использует встроенную IPv4-сеть:
пул `10.203.0.0/16`, выход NAT44 через `eth0` внутри контейнера и DNS `1.1.1.1`.
Пул должен не пересекаться с сетями установки; конфликт вызывает явную ошибку.

`SKVOZ_NETWORK` необязательна и заменяет встроенную сетевую конфигурацию целиком.
Добавьте её в `environment`, если нужны другой пул, DNS, выходной интерфейс,
routed IPv4/IPv6 или явный inventory gateway, описанный ниже. Например:

```yaml
SKVOZ_NETWORK: '{"ipv4":{"pool":"10.204.0.0/16","egress":"nat44","interface":"eth0"},"dns_servers":["9.9.9.9"]}'
```

Значение `'{}'` явно отключает IP backend, сохраняя TCP-соединения.
При запуске с `--config` действуют настройки файла: отсутствие поля `network`
выбирает ту же встроенную IPv4-сеть, а `"network": {}` отключает IP backend.

Шаблон ограничивает контейнерные журналы через `json-file`: до трёх файлов
по 10 MB. Это не ограничивает системные журналы и данные постоянного тома;
их объём и резерв свободного места контролируются отдельно.

Сборка в CI и локальная сборка используют один
[Dockerfile](../connectors/server/Dockerfile). В нём runtime и helper компилируются
командой Cargo на промежуточном этапе Rust, gems устанавливаются в Ruby Bookworm,
а готовые бинарники и библиотеки копируются в итоговый образ Ruby Bookworm.
Этап `base` содержит gems и Ruby-код; `tests` добавляет RSpec, тестовые данные
и бинарники runtime/helper, NATS и локального центра ACME Pebble;
`deploy` — итоговый образ сервера. CI запускает проверки этапа `tests` и собирает
`deploy` для GHCR; локальная сборка через Compose также выбирает `deploy`.
В GitHub Actions проверки и публикация выполняются в одном job с общим
Buildx builder: этапы `tests` и `deploy` переиспользуют сборку ядра и библиотеки Ruby для запуска.
После проверки `deploy` через Compose публикация использует кеш этого же builder,
без повторной компиляции ядра и установки gems. В pull request выполняются
проверки без публикации.
Ruby уже входит в базовый образ. На машине сборки нужны Docker/Compose;
устанавливать Ruby, Rust или Python для сборки сервера не требуется.
Такое разделение этапов сохраняет компиляторы внутри сборки.

Тот же образ можно собрать локально из публичных исходников:

```sh
sudo docker compose -p skvoz --project-directory "$PWD/connectors/server" \
  -f /etc/skvoz/compose.yaml -f connectors/server/compose.source.yaml up -d --build
```

Используйте ту же настроенную копию YAML. `--project-directory` задаёт базовый
каталог для относительного build context; дополнительный файл выбирает образ
`skvoz-server:source` и этап `deploy`. При сборке
зафиксированы digest образов сборщика и среды выполнения, SHA256 официального
release-архива NATS2.15.1, а также
версии зависимостей в Cargo/Gem lock-файлах. Целевая платформа образа — `linux/amd64`; новая gateway-интеграция и другие
архитектуры требуют отдельной проверки.

## Вход и промежуточные NATS брокеры

Установка stock входа, public-key bootstrap, независимые credentials и штатные
TLS/renewal timers описаны в [комплекте deploy/nats](../deploy/nats/README.md).

Полный сервер остаётся самостоятельным: пользователи, выдача устройств,
Auth Callout, Core/network и TCP/IP-выход принадлежат одному серверу. Его peer ID
равен0. Дополнительный вход — stock NATS с собственным TLS и ACME состоянием;
Ruby/Core/network/helper на нём не нужны. Полный сервер инициирует исходящие
native leaf connections; трафик по ним двусторонний. Account APP переносит
пользовательские и Core/enrollment сообщения, AUTH — encrypted Auth Callout,
SYS — защищённые служебные queries/events/KICK. Клиент подключается только к APP.

`skvoz-server auth-public --state /var/lib/skvoz` возвращает публичные issuer/xkey
и namespace для настройки брокера. Приватные signing/curve seeds остаются только
на полном сервере. На входе нужны отдельные native LEAFNODE users в owning
APP/AUTH/SYS accounts с receiver-enforced ACL и явным bootstrap exemption
`auth_users`. Обычные пользователи получают индивидуальные JWT/ACL от Callout;
общий клиентский service password не используется. Служебные STANDARD credentials
имеют отдельные accounts/разрешения и работают без доступного Callout.

Полный сервер читает приватный `leaf_remotes` JSON версии1:

```json
{"v":1,"entries":[{"address":"entry.example.org","port":7422,"ca_pem":null,
"users":{"APP":{"username":"__skvoz_leaf_app","password":"<independent 43-character secret>"},
"AUTH":{"username":"__skvoz_leaf_auth","password":"<independent 43-character secret>"},
"SYS":{"username":"__skvoz_leaf_sys","password":"<independent 43-character secret>"}}}]}
```

Каждый account/вход имеет независимый случайный пароль; usernames внутри входа
различаются. Файл принадлежит UID10001, режим0600 в каталоге0700. Допускается
до8 endpoints. Это транспортный список, без node IDs, remote authority, pool,
выбора выходов или перемещения живых TCP сеансов. `client_address` и `client_ca`
задают публичный адрес/доверие входа для выдаваемого клиентского профиля;
`address` остаётся TLS identity собственного брокера полного сервера.

Внутренний полный сервер может использовать loopback bind с `tls.mode=local`
и не публиковать порты. Каждый публичный брокер выпускает и обновляет свой
сертификат и ключ; публичный профиль — shortlived160h. Локальный issuer полного
сервера не заменяет проверку публичного сертификата входа.

### Полный сервер без публичных listener

Используйте отдельный [Compose шаблон](../connectors/server/compose.internal.yaml)
и [JSON конфигурацию](../connectors/server/server.internal.example.json).
Это тот же полный сервер с IPv4 NAT44; NATS слушает loopback внутри контейнера,
а native leaf connections исходят к входу. Публичные порты80/4222 ему не нужны.

```sh
sudo install -d -m 0700 /etc/skvoz
sudo install -d -m 0700 /etc/skvoz/config
sudo install -d -m 0700 -o 10001 -g 10001 /etc/skvoz/leaf
sudo install -m 0600 connectors/server/compose.internal.yaml /etc/skvoz/compose.yaml
sudo install -m 0600 connectors/server/server.internal.example.json /etc/skvoz/config/server.json
sudo docker compose -p skvoz -f /etc/skvoz/compose.yaml up -d
sudo docker compose -p skvoz -f /etc/skvoz/compose.yaml exec -T --user 10001 server skvoz-server auth-public
```

Сначала `leaf_remotes: null` позволяет получить public keys без готового входа.
Настройте вход по [его инструкции](../deploy/nats/README.md), затем установите
полученный private transport файл как `/etc/skvoz/leaf/leaf-remotes.json`
(UID/GID10001,0600). В `config/server.json` замените `leaf_remotes` на
`/etc/skvoz-leaf/leaf-remotes.json`, `client_address` на публичный адрес входа
и `advertised_port` на его клиентский порт. При публичном сертификате входа
`client_ca` остаётся `null`; при private CA задайте путь к её PEM в mounted
leaf каталоге с теми же private permissions. Перезапустите server после
этого изменения конфигурации и проверьте `skvoz-server health`.
Пользователей добавляйте после выбора окончательного клиентского endpoint.

Для entry-first создайте в `/etc/skvoz/leaf/bootstrap.json` копию JSON
конфигурации с UID/GID10001 и0600, пока `leaf_remotes` ещё `null`. До старта
сервера получите public keys в том же state volume:

```sh
sudo docker compose -p skvoz -f /etc/skvoz/compose.yaml run --rm --no-deps --user 10001 server auth-public --offline --config /etc/skvoz-leaf/bootstrap.json
```

Затем настройте вход, установите transport и конечный endpoint в root-owned
`config/server.json` и запустите Compose. При миграции сначала выполните
offline conversion в этот state volume; `auth-public --offline` не снимает
startup security fence. Старые входы должны быть остановлены до cutover.

## Автообновление готового образа

В комплекте есть небольшие [скрипты развёртывания](../connectors/server/deployment/):
`auto-update-enable.sh` устанавливает systemd service/timer на серверной машине,
`auto-update-disable.sh` выключает их. Для этого нужны Bash, systemd,
Docker/Compose и права root. На одной машине поддерживается один такой таймер
для одного развёртывания SKVOZ. Это дополнение к запуску готового образа из
registry; локальная сборка через `compose.source.yaml` обновляется отдельно.

Используйте тот же настроенный YAML, с которым сервер уже запущен.
Если используются дополнительные Compose overlays, сохраните объединённый
Compose в один приватный YAML, проверьте его и передайте этот же файл таймеру.
Стандартные NATS входы обслуживаются собственными systemd units из `deploy/nats/`.

Для отслеживания новых сборок укажите в поле `image` изменяемый тег,
например используемый по умолчанию `ghcr.io/efreh/skvoz/skvoz-server:latest`. Digest и уникальный тег
версии или commit не переключают сервер на следующий выпуск. Доступ к закрытому
registry настраивается через `sudo docker login`: фоновые команды запускаются
от root.

После запуска standalone включите автообновление из корня исходного репозитория:

```sh
sudo bash connectors/server/deployment/auto-update-enable.sh /etc/skvoz/compose.yaml skvoz
```

Для входа или отдельного выхода передавайте подготовленный
`/etc/skvoz/compose.resolved.yaml`, с которым запущено это развёртывание.

Интерфейс скрипта — `auto-update-enable.sh [COMPOSE_FILE [PROJECT_NAME]]`.
Без аргументов он использует `compose.yaml` рядом с каталогом `deployment/`;
имя проекта можно опустить, если запуск Compose использовал имя по умолчанию.
Для явно заданного при запуске `-p` передавайте то же имя вторым аргументом.
Скрипт проверяет, что `server` уже запущен, сохраняет фактическое имя проекта
Compose и устанавливает `skvoz-auto-update.service`/`skvoz-auto-update.timer`.
Путь к YAML сохраняется как абсолютный; сохраните файл на машине.
Фоновые команды перечитывают YAML и отключают автоматическую загрузку `.env`;
конфигурация должна быть полной и не зависеть от `export` в интерактивном терминале.
Включение повторяемо: повторный запуск заменяет настройки таймера.

Передавайте полную конфигурацию развёртывания одним YAML-файлом. Если используете
переопределения `-f`, сначала подготовьте единый файл через
[`docker compose config`](https://docs.docker.com/reference/cli/docker/compose/config/)
и запускайте сервер и автообновление с ним под тем же именем проекта.
Изменение имени проекта создаёт другое развёртывание и другой том состояния.

Первый опрос выполняется через 2 минуты после включения таймера; следующий —
через 2 минуты после завершения предыдущей попытки. Попытки не перекрываются.
Обновлятор выполняет `pull --quiet server`, затем
`up -d --no-deps --no-build --pull never server`. При изменении образа Compose
пересоздаёт контейнер с тем же томами состояния, портами и настройками;
при неизменном образе контейнер продолжает работать.
Пересоздание прерывает текущие TCP/IP-сессии. Ошибка загрузки образа оставляет
работающий контейнер прежним; остановленный вручную сервер не запускается.
Автоматический откат после ошибки запуска нового образа не выполняется.

Штатный вывод подавляется, в журнал записываются только ошибки задачи.
Проверить таймер и прочитать ошибки:

```sh
sudo systemctl list-timers skvoz-auto-update.timer
sudo journalctl -u skvoz-auto-update.service -p err
```

Выключение останавливает таймер и текущую задачу обновления. Оно сохраняет
установленные файлы, конфигурацию и работающий сервер:

```sh
sudo bash connectors/server/deployment/auto-update-disable.sh
```

Остановка задачи в момент пересоздания контейнера не отменяет уже выполненные
Docker операции. Для последующей ручной остановки сервера сначала выключите
автообновление. Перед изменениями YAML также выключите таймер,
проверьте конфигурацию и включите его снова.

## Пользователи NATS и устройства

Эти команды обращаются к закрытому сокету администрирования сервера и доступны
процессам с тем же UID. Для Compose:

```sh
sudo docker compose -p skvoz -f /etc/skvoz/compose.yaml exec -T --user 10001 server skvoz-server user add alice
sudo docker compose -p skvoz -f /etc/skvoz/compose.yaml exec -T --user 10001 server skvoz-server user list
sudo docker compose -p skvoz -f /etc/skvoz/compose.yaml exec -T --user 10001 server skvoz-server user show alice
sudo docker compose -p skvoz -f /etc/skvoz/compose.yaml exec --user 10001 server skvoz-server user device-add alice --prompt-password
sudo docker compose -p skvoz -f /etc/skvoz/compose.yaml exec -T --user 10001 server skvoz-server user reset-password alice
sudo docker compose -p skvoz -f /etc/skvoz/compose.yaml exec -T --user 10001 server skvoz-server user remove alice
```

Команды `add` и `reset-password`
генерируют пароль, если он не передан через `--password-stdin` или
`--prompt-password`. Пароль должен содержать 12–72 байта UTF-8 без NUL; передача
пароля аргументом командной строки не поддерживается. Сгенерированный пароль
появляется один раз в результате команды; постоянное состояние хранит хеш bcrypt.
Команды `list` и `show` не раскрывают пароли. Для экспорта устройства нужен пароль учётной записи.

`add` возвращает данные подключения первого устройства. `device-add`
сначала сохраняет новый ID из выделяемого сервером пула учётной записи,
затем возвращает профиль устройства. По умолчанию резервируется 8 ID
на учётную запись и128 IDs суммарно, до64 на пользователя. Полный ACL всех
резервируемых IDs должен помещаться в encrypted response≤65588B: namespace256
с64 устройствами превышает этот бюджет и отклоняется до изменения состояния.
Запрос Callout≤16KiB; headers≤4KiB, queue16 и один BCrypt worker остаются фиксированы.
Удаление пользователя освобождает активную ёмкость. Ранее выданные ID
не используются повторно: проверяемый 64-битный счётчик только растёт.
Исторические ID не занимают активный лимит навсегда.
Общий логин означает общий доступ к уже назначенным ID; резерв пула до назначения не разрешён Core ACL.
Для ручного использования демона каждому устройству нужен собственный экспортированный профиль. Повторное
использование ID другим поколением не вытесняет работающий экземпляр:
Core допускает новую попытку только после завершения прежнего поколения.
Выбирать случайный ID и повторять попытки небезопасно.

JSON-профиль версии 4 содержит внешние `address`/`port`, credentials,
namespace и выделенный peer_id, а также точные версии network/runtime/Core.
Профиль с private CA содержит `ca_pem`; сохраняйте его в закрытом файле.
Пароль и экспорт защищайте как учётные данные. Клиент Ubuntu принимает
IP/DNS, внешний порт и NATS credentials, затем выделяет устройство автоматически
через [enrollment v4](ubuntu-client.md).
Назначения CLI и enrollment входят в один пул. Неподдерживаемые версии
отклоняются явно; старые форматы не мигрируются автоматически.

ACL каждого подключения разрешает только назначенные ID текущего пользователя,
Core peer0 и его enrollment subjects. Add/reset/remove не перезагружают NATS.
Новая durable ревизия и локальный authoritative device set применяются до
успешного ответа. Reset-password выдаёт новые monotonic IDs, сохраняет device
tokens с новым ID и меняет access identity; remove→add того же login не использует
старую identity. Старые IDs не могут восстановить доступ после reconnect.

Адресный SYS KICK связывает serverID/CID с авторитетной identity подключения.
Достижимые отозванные подключения закрываются в пределах5s, локальные ресурсы —
3s. При потере SYS leaf физический socket удалённого брокера может оставаться;
результат содержит число известных `pending_connections` и число неподтверждённых
SYS-путей `unverified_entry_paths`. Ноль известных CID после перезапуска приложения
не доказывает закрытие удалённых sockets: `reconciliation_complete` остаётся false
до обнаружения всех настроенных SYS leaf и полного авторитетного прохода. Старые IDs уже запрещены локально,
отключение повторяется после восстановления SYS. Чужие здоровые подключения
не пересоздаются. Неопределённый локальный ACK оставляет durable startup fence
и запрещает дальнейшие изменения до восстановления. Потеря целого APP пути
завершает обычный клиентский path по timeout10s плюс cleanup3s.

Состояние версии2 автоматически не принимается. До несовместимого обновления
остановите таймер автообновления и закрепите текущий image digest. Остановите старый сервер,
сохраните private backup, подготовьте согласованные версии сервера и клиентов
и выполните от владельца UID10001:

```sh
skvoz-server migrate-state --source /absolute/private/old-state --config /absolute/private/new-server.json
```

Конфигурация версии5 задаёт новый, ещё не существующий `state_dir`. Конвертация
берёт exclusive source lock, сохраняет users/hashes/reserved and assigned IDs,
device tokens и monotonic allocator; удаляет старые роли/node credentials и
создаёт новые независимые bootstrap/callout keys. TLS копируется только при
валидных key/trust/SAN/expiry и принадлежности файлов source state. Старое
состояние не перезаписывается. До SYS reconciliation и локального ACK действует
startup fence.
Полный сервер при запуске начинает с пустого локального набора допуска и закрытого
Auth Callout. Большой inventory обрабатывается ограниченными страницами с продолжением
между проходами; до завершения сверки readiness остаётся false. Только подтверждённый
локальный ACK текущего набора устройств снимает durable fence. Потеря SYS оставляет
сервер работающим с запретом доступа, чтобы сверка продолжилась после восстановления.
Старый брокер и его legacy client credentials должны быть удалены
из serving configuration при cutover; преобразование файла само по себе не
меняет внешние машины. Клиенты переходят на согласованный enrollment4/network6.

## Конфигурация и мосты к внутренним сервисам

`serve --config /absolute/private/server.json` читает строгий JSON версии 5.
При root bootstrap файл принадлежит root, имеет права `0600` в закрытом
каталоге; конфигурация непривилегированного host создаётся bootstrap отдельно.
Неизвестные и повторные ключи, неверные типы и старые версии отклоняются.
Без файла начальные настройки создаются из environment Compose.

| Ключ | Назначение |
| --- | --- |
| `v` | 5. |
| `leaf_remotes` | `null` для standalone либо путь к приватному transport JSON1. |
| `state_dir` | `/var/lib/skvoz`, закрытое состояние host. |
| `address`, `advertised_port` | Внешний IP/DNS и внешний порт; порт по умолчанию равен `port`. |
| `bind`, `port`, `monitor_port` | Брокер `0.0.0.0:4222`, мониторинг loopback:8222. |
| `client_address`, `client_ca` | Адрес и trust входа для client exports; по умолчанию собственный address/CA. |
| `namespace` | `skvoz.application`; совпадает с постоянным состоянием. |
| `runtime_binary`, `helper_binary`, `nats_binary` | Комплектные runtime/helper 0.6.0 и NATS 2.15.1. |
| `max_identities`, `devices_per_user` | 128 и 8; конечные лимиты активных устройств. |
| `admin_timeout`, `stop_timeout` | 5 и 8 секунд; административный запрос и остановка дочерних процессов. |
| `allow`, `deny` | До 128 правил `{cidr,protocols,ports}`. |
| `network` | Backend, DNS, source grants и inventory gateway. |
| `tls` | ACME или готовые проверяемые сертификаты входа; loopback bind также допускает явный local issuer. |

В `network` допустимы `ipv4`, `ipv6`, `dns_servers`, `service_prefixes`,
`server_addresses`, `management_endpoints`, `max_mtu`, `channels`.
Backend — `null` либо `{pool,egress,interface}`. IPv4 использует NAT44 или
routed с каноническим /16–/24; встроенный профиль задаёт 10.203.0.0/16, `nat44`, `eth0`.
IPv6 требует явно маршрутизируемый /64 и `routed`, работающий uplink и обратный
маршрут; NAT66 и автоматическое исключение IPv6 не предусмотрены.
У выбранного IPv6-интерфейса должны быть адрес без незавершённого DAD и
маршрут по умолчанию в namespace сервера; helper проверяет их перед запуском.
Оба `null` дают TCP-only сервер без TUN/helper. DNS содержит 1–4 numeric адреса
доступных семейств, или пустой список у TCP-only backend.
MTU по умолчанию 1500, каналы — 1. Общие фиксированные лимиты находятся в
[сетевом контракте](network-runtime.md#данные-кредит-и-границы).

Compose задаёт IPv4 forwarding и fragment limits 4 МиБ / 15 секунд.
При включении IPv6 оператор также задаёт `net.ipv6.conf.all.forwarding=1`,
`net.ipv6.ip6frag_high_thresh=4194304` и `net.ipv6.ip6frag_time=15`.
Если ядро предоставляет `net.netfilter.nf_conntrack_frag6_high_thresh` и
`net.netfilter.nf_conntrack_frag6_timeout`, их значения также должны быть
не выше 4194304 и 15 соответственно. Helper проверяет namespace settings
и не исправляет их скрыто.
Pool conflicts, uplink и обратная маршрутизация должны быть проверены в реальном
окружении. Успех парсинга конфигурации этого не доказывает.

Пример явного разрешения обычного TCP-сервиса `127.0.0.1:8081`:

```json
{"allow":[{"cidr":"127.0.0.1/32","protocols":[6],"ports":[8081]}]}
```

Это фрагмент полной конфигурации. `protocols` — `"any"` либо unique список
IP protocol numbers 0–255. `ports=null` или список 1–65535, только при одном
protocol 6/17. Deny имеет приоритет перед allow. Частные и локальные назначения
нуждаются в явном allow; неопределённые, multicast и broadcast недопустимы.
Management endpoints запрещены независимо от allow. Другой обычный сервис
на адресе сервера допускается явно. Loopback-разрешение относится к TCP-сокету
обычного сервиса: IP-шлюз не маршрутизирует пакеты с loopback-назначением.
Management-порты недоступны через другие loopback-адреса даже при широком allow.

`server_addresses` содержит до 32 numeric адресов, включая internal/public/NAT
aliases. `management_endpoints` — до 32 `{address,protocol,port}`, protocol 6/17.
Host дополняет inventory адресами namespace, advertised broker и ACME endpoints;
дополнительные published/NAT aliases обязан указать оператор. Wildcard publish
не заменяет полный inventory. `service_prefixes` — до 128 `{peer,prefix}`;
они не должны пересекаться с lease pools, защищёнными сетями и друг с другом.
Клиент не назначает себе произвольный prefix.

Все DNS-ответы назначения проверяются до выбора numeric адресов; private/public
смесь не получает скрытого обхода policy. Повторное DNS resolution при connect
не выполняется. Host задаётся каноническим IP или ASCII A-label; зоны и
неоднозначные адреса отклоняются. Байты TCP-сервиса остаются непрозрачными.

Конфигурация helper хранится в `/var/lib/skvoz-network/helper.json`.
Отдельный каталог `/var/lib/skvoz-network/state` содержит постоянный lease
registry 1 и journal: первый запуск требует пустого state directory.
Registry принадлежит helper:
каталог root `0700`, файлы `0600`, no-follow, atomic replacement и fsync.
Назначение остаётся за peer после отключения; tombstones не передаются другому
peer автоматически. Missing/corrupt registry после инициализации блокирует
backend. После полной утраты состояния восстановите резервную копию либо
выберите новый pool; старые адреса нельзя считать безопасно свободными.

Объект `tls` поддерживает `mode:acme`, HTTPS-адрес `directory`, `email`,
явное `terms_agreed:true`, IP-адрес `challenge_host`, `challenge_port` (8080),
`challenge_public_port` (80), `order_timeout` (120 секунд) и
`renewal_interval` (86 400 секунд, допустимы 1–86 400).
Для локального центра сертификации можно указать закрытые файлы
`issuer_ca` и `certificate_ca`. Учётная запись ACME привязана к каталогу
своего центра; использование этой учётной записи с другим центром отклоняется.
Сервер требует профиль `shortlived`, выполняет настоящую проверку HTTP01
и перед активацией проверяет SAN, соответствие ключа, назначение серверного
сертификата, цепочку и срок действия. Продление начинается при остатке
срока 48 часов. Ошибка выпуска не позволяет активировать непроверенный
сертификат. Если при перезапуске сертификат истёк, сервер автоматически
выпускает замену. Хранятся только текущая и предыдущая неизменяемые версии
TLS-материалов, а также материалы незавершённой операции.
Активация нового сертификата выполняет штатную перезагрузку NATS и может
закрыть активные подключения и TCP-сессии. Новые подключения используют
новый сертификат; прозрачное восстановление закрытых потоков не гарантируется.

`mode:provided` требует закрытые файлы `certificate` и `key` по абсолютным
путям; дополнительно можно задать `ca`. Их родительские каталоги должны
иметь права `0700`. Материалы копируются в неизменяемую закрытую версию
и проверяются до активации. В этом режиме администратор поддерживает
срок действия исходных сертификатов; автоматическое продление через ACME
работает в режиме ACME. Внутреннее подключение проверяет внешний адрес
в SAN через [параметр runtime `tls_server_name`](nats-runtime.md),
сохраняя SNI из URL NATS и проверку доверия.

## Метаданные TCP и жизненный цикл

Network metadata 6 различают `tcp`, `ip-session`, `ip-data`:

```json
{"v":6,"type":"tcp","host":"example.org","port":443}
```

Проверку назначения, native connect, ACCEPT, partial SEND, CONSUME после записи
и двусторонний EOF выполняет общий Rust runtime. Ruby не разбирает эти метаданные
и не копирует payload. IP-сессия получает durable source grants, все связанные
каналы и подтверждённый helper setup до ACTIVE; server обслуживает TCP/IP
клиентов одновременно. Детальные границы — в [общем контракте](network-runtime.md).

Root bootstrap передаёт helper channel исключительно runtime. Потеря owner,
смерть runtime/helper или неподтверждённая очистка завершает контейнер;
непривилегированный host не создаёт новый root helper. Изменение локального DeviceSet
закрывает ресурсы удалённых устройств и сохраняет соединения остальных клиентов.
Потеря собственного Core или APP-транспорта после готовности требует ограниченной
очистки и перезапуска контейнера.
Helper допускает конечное ожидание подготовки состояния и TLS до передачи
канала runtime; сроки handshake и обработки кадров описаны в
[контракте helper](../network/helper/README.md#запуск-и-привилегии).
Дочерние процессы защищены от смерти родителя; штатная остановка использует
общий срок, затем принудительно завершает и собирает их. Сохранённый PID
не используется без проверки владельца процесса.

## Регрессионные проверки

Основные проверки Core и демона принадлежат общему стенду проекта:

```sh
python3 testbench/run.py check
```

Проверки серверного коннектора написаны на Ruby/RSpec. Этап `tests` содержит
всё для контрактных и реальных процессных проверок сервера, TLS и ACME:

```sh
docker build --target tests -f connectors/server/Dockerfile -t skvoz-server:tests .
docker run --rm --cap-drop ALL --security-opt no-new-privileges:true skvoz-server:tests
```

Для разработки `bundle exec rspec spec` из `connectors/server` запускает
детерминированные проверки. Реальные сценарии включаются через
`SKVOZ_INTEGRATION=1`; бинарники задаются через `SKVOZ_TEST_RUNTIME`,
`SKVOZ_TEST_HELPER`, `SKVOZ_TEST_NATS`, `SKVOZ_TEST_PEBBLE`.
Самостоятельные проверки generic daemon/TLS могут дополнительно использовать
`SKVOZ_TEST_CORE`; этот демон не входит в продуктовый путь сервера.
Compose-сценарии используют `SKVOZ_COMPOSE=1` и `SKVOZ_TEST_IMAGE`.
Process-сценарии требуют UID/GID 10001, пустые capabilities и
`no-new-privileges`. Штатный tests-образ задаёт UID/GID 10001;
capabilities и `no-new-privileges` задаются флагами запуска выше.
При отказе CLI журнал содержит событие `command_failed` с классом ошибки,
фиксированным кодом и basename/строками исходника, без текста исключения
и содержимого конфигурации.
При отказе runtime поле `failure` в health сохраняет подтверждённую причину
из `RUNTIME_STATE`, например `runtime_network_unavailable`. Журнал содержит
`runtime_state_failed`; неожиданное завершение дочернего процесса даёт
`supervised_exit` с кодом выхода или номером сигнала, например
`network_signal_9`. Эти события используют фиксированные коды и не копируют
stderr дочерних процессов, credentials или пользовательский трафик.
Наличие fixtures/workflow означает воспроизводимый путь проверки, а не
завершённую квалификацию нового runtime.

Проверки должны включать реальные назначения и EOF, policy/management isolation,
малые очереди, отзыв доступа, восстановление, root capability drop, сохранение
локального device set и только собственную cleanup. TLS/ACME проверяются отдельно от gateway.
Локальный Pebble не доказывает доступность HTTP01 из Интернета и доверие
публичного сертификата. Remote CI, GHCR push/pull и реальные IPv6 маршруты
также проверяются отдельно. CI публикует образ только после своих проверок.

## Совместимость транспорта

Брокер, runtime, enrollment и probes используют
[общий профиль транспорта](nats-runtime.md#профиль-транспорта).
Сервер7.0.0 требует enrollment4, network6, API1, runtime/helper0.6.0
и Core4.1.0. Конфигурация —5, persistent state —3, startup —3; Core wire2
и ABI1 сохраняются. Постоянных legacy wire/config fallbacks нет.
Ограниченная offline конвертация authority state2 описана выше; runtime
не запускается с прежней node/pool configuration. Клиентский profile4 задаёт
allowed/initiate[0] и точный набор версий. Несовместимые комбинации явно
отклоняются; открытые TCP/IP сеансы прежнего runtime не переносятся.
