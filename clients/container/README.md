# Контейнерный клиент SKVOZ

Headless клиент Linux/amd64 для приложений и выбранных контейнеров. Ruby host
управляет тем же Rust Core/network runtime, который используют остальные клиенты.

Выберите VPN для контейнеров в общем network namespace либо совместные явные
HTTP/CONNECT/SOCKS5-входы, именованные TCP forwards и HTTP base-URL gateways.
Обычные адрес сервера, логин и пароль работают напрямую и через поддерживаемый
стандартный NATS-вход.

Начните с [руководства](../../docs/container-client.md) и
[одного Compose-файла](compose.yaml). Три поля подключения редактируются в inline
YAML; обязательных внешних env/config файлов нет.

Из корня репозитория, с Docker BuildKit:

```sh
docker build -f clients/container/Dockerfile --target deploy -t skvoz-client:local .
```

Компонентные тесты на Ruby 4.0.7 используют точный lock:

```sh
cd clients/container
bundle install
bundle exec rspec spec
```

`skvoz-client serve` читает `SKVOZ_CLIENT_CONFIG`; `check` проверяет YAML без
подключения; `health` проверяет текущий процесс и heartbeat; `--version` показывает
версии компонента и протоколов. Образ запускает `serve` и содержит health check.
Private named volume сохраняет одну device identity. Ограничения streaming,
привилегии и правила restart/recreate описаны в руководстве.
