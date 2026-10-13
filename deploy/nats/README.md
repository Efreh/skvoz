# Стандартный NATS вход

Здесь находится комплект для дополнительного входа или промежуточного native NATS
узла. Полный сервер управляет пользователями и устройствами, выполняет Auth Callout
и TCP/IP-выход. На входе нужны только stock NATS2.15.1, lego5.5.2, Bash, OpenSSL,
GNU coreutils, util-linux и systemd. Ruby, SKVOZ Core/network и egress helper здесь
не устанавливаются. Каждый публичный узел имеет собственные сертификат, ACME
account и три native leaf credentials.

## Установка штатных инструментов

Установите официальные release archives с проверкой опубликованных checksums.
Для NATS2.15.1 Linux amd64:

```sh
curl --fail --location --output nats.tar.gz https://github.com/nats-io/nats-server/releases/download/v2.15.1/nats-server-v2.15.1-linux-amd64.tar.gz
printf '%s  nats.tar.gz\n' bf5edc7b1ce98e885c368cb43a886eb148dc5d0551d99f3cf6141fb5e1954c70 | sha256sum --check
tar -xzf nats.tar.gz
sudo install -m 0755 nats-server-v2.15.1-linux-amd64/nats-server /usr/local/bin/nats-server
nats-server --version
```

Для Linux arm64 имя архива — `nats-server-v2.15.1-linux-arm64.tar.gz`, SHA256 —
`e2fbab4cad7d3c76e3052eb959570393c9d73ba756b70bf8e9eea61e9ae6d49c`.
Остальные архитектуры проверяйте по официальному `SHA256SUMS` этого release.
Установите lego из [официального release5.5.2](https://github.com/go-acme/lego/releases/tag/v5.5.2)
с проверкой его checksums и проверьте `lego --version`.

```sh
sudo useradd --system --no-create-home --shell /usr/sbin/nologin skvoz-nats
sudo install -d -m 0700 -o skvoz-nats -g skvoz-nats /var/lib/skvoz-nats
sudo install -d -m 0700 /etc/skvoz-nats
sudo install -d -m 0755 /usr/local/lib/skvoz-nats
sudo install -m 0755 deploy/nats/*.sh /usr/local/lib/skvoz-nats/
sudo install -m 0644 deploy/nats/*.service deploy/nats/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

## Bootstrap без зависимости от уже живого входа

Есть два порядка. Для нового самостоятельного полного сервера сначала запустите
его обычный Compose и получите public keys через `skvoz-server auth-public`.
Затем настройте вход и добавьте его `leaf-remotes.json` в полный сервер.
Для полного сервера без публичных портов используйте
[internal Compose и bootstrap](../../docs/server-connector.md#полный-сервер-без-публичных-listener).

Для entry-first или offline migration остановите старый полный сервер и старые
входы, выполните [миграцию состояния](../../docs/server-connector.md#совместимость-транспорта),
затем, от непривилегированного владельца состояния, выполните:

```sh
skvoz-server auth-public --offline --config /etc/skvoz/server.json
```

Команда блокирует state, создаёт новые ключи для свежего state либо возвращает
public keys существующего state3. Она выводит только `issuer`, `xkey` и `namespace`;
private seeds и service credentials читать вручную не требуется. Migration startup
fence остаётся установленным. Создайте вход по этим public keys, передайте его
служебный transport файл полному серверу и запустите оба компонента. Полный сервер
проверяет SYS reconciliation всех настроенных входов и локальный набор устройств
до допуска клиентов. Остановленные старые входы заменяются новым stock config;
оставлять работающий старый issuer при сохранённых device IDs небезопасно.

Создайте `/etc/skvoz-nats/entry.env` (root,0600):

```ini
SKVOZ_ISSUER=<public account NKey>
SKVOZ_XKEY=<public curve NKey>
SKVOZ_NAMESPACE=skvoz.application
SKVOZ_ADDRESS=entry.example.org
SKVOZ_CLIENT_PORT=4222
SKVOZ_LEAF_PORT=7422
SKVOZ_ACME_EMAIL=operator@example.org
SKVOZ_ACME_TERMS_AGREED=true
```

Запустите configure один раз от `skvoz-nats`, передав эти переменные. Например,
root может загрузить доверенный env-файл и передать public значения через `env`:

```sh
sudo bash -c 'set -a; source /etc/skvoz-nats/entry.env; set +a; exec runuser -u skvoz-nats --preserve-environment -- /usr/local/lib/skvoz-nats/configure.sh'
sudo systemctl start skvoz-nats-renew.service
sudo systemctl enable --now skvoz-nats.service skvoz-nats-renew.timer
```

DNS/IP должен указывать на этот вход, HTTP01 должен достигать его port80.
Конфигурация entry содержит отдельные APP/AUTH/SYS leaf-only users с receiver ACL.
Их bootstrap не зависит от удалённого Auth Callout. Клиенты не знают эти credentials;
обычный клиент не может аутентифицироваться leaf-only identity или перейти в SYS/AUTH.
Ограничьте native leaf port firewall-правилом для полного сервера.

Перенесите `/var/lib/skvoz-nats/leaf-remotes.json` по защищённому каналу в приватный
файл полного сервера (владелец его state,0600) и задайте `SKVOZ_LEAF_REMOTES`.
Для нескольких входов соберите `entries` из их файлов, до8. Не используйте один
комплект credentials на разных узлах. Это транспортный список; payload всегда
идёт к единственному серверному peer0. Полный сервер инициирует три native remotes;
APP данные, AUTH запросы/ответы и SYS операции двусторонние.

## Сертификаты, renewal и отказ

Первый выпуск и последующие проверки используют standard lego HTTP01 и публичный
профиль `shortlived` (не более160h). Каждый узел сохраняет собственный ACME account
в `/var/lib/skvoz-nats/acme`. Таймер проверяет оставшиеся48h дважды в сутки
(00:00 и12:00, jitter до15min, AccuracySec1s); failure включает
отдельный retry timer через5min, повторная failure снова запускает его. Успех
выключает retry. Проверяйте `systemctl list-timers 'skvoz-nats-*'` и журнал renewal.

Если первый выпуск прервался до регистрации аккаунта и lego затем подтвердил
`accountDoesNotExist`, незарегистрированные ключ и метаданные текущего issuer/email
сохраняются в приватном `acme/unregistered-*`. Следующий штатный retry создаёт
новый аккаунт. Зарегистрированные аккаунты и сертификаты не сбрасываются;
обычная ошибка сети не вызывает сброс. Каждая попытка сохраняет предел150s.

Apply проверяет SAN/цепочку/ключ/actual dates, создаёт полную новую generation,
атомарно переключает ссылку и выполняет штатный HUP. За5s TLS-first leaf probe
должен подтвердить новый serial с проверкой trust/SAN. Отказ проверки config до
HUP возвращает предыдущую ссылку и сохраняет работающий брокер. После HUP rollback
подтверждает прежний действующий serial и восстанавливает его actual expiry timer;
если это не удаётся подтвердить, брокер останавливается для восстановления.
Штатный HUP при обновлении TLS может закрыть активные подключения и TCP-сессии.
Новые подключения проверяют новый сертификат. Сохранение потоков и прозрачное
восстановление закрытых сессий не гарантируются.

Отдельный standard systemd timer привязан к actual expiry и serial подтверждённого
serving поколения. Его oneshot проверяет generation под короткой activation lock;
stale timer не отключает заменивший сертификат. Actual expiry закрывает брокер через
systemd; issuance или apply не обходят эту проверку. ExecStartPre запрещает запуск с истёкшим материалом. Failed renewal
сохраняет ещё действующий сертификат и retries; после expiry доступ не продолжается.
Для восстановления запустите renewal и затем `systemctl start skvoz-nats.service`.
Внутреннему полному серверу не нужны публичные порты только ради одинакового TLS:
для него допустима локальная CA с явным доверием к его адресу.

Service lifecycle и renewal управляют только этим NATS. Перед изменением issuer,
leaf credentials или account config согласуйте restart и обновление transport
файла полного сервера; обычные add/reset/delete пользователей выполняются там без
перезапуска входа. Не копируйте state/ACME keys между публичными узлами.
