# SKVOZ Core daemon

`skvoz-core-daemon` — Linux executable, встраивающий ту же универсальную
[Core library](../core/README.md) и `NatsRuntime`. Приложения Ruby, Python и других
языков используют один локальный [IPC v1](../docs/daemon-ipc.md), без native bindings.
Каталог выделяет process/config/socket wrapper; отдельного client/server ядра нет.

Из корня репозитория:

```sh
cargo build --release -p skvoz-daemon --locked
./target/release/skvoz-core-daemon --help
python3 testbench/run.py daemon
```

Последняя команда квалифицирует независимые release daemons и Python/Ruby
процессы через временный INFO → TLS NATS. Нужны Linux, Rust1.92+, Python3.9+,
Ruby3.4+, Docker и OpenSSL. `--offline` использует заранее cached зависимости/image.

Для собственного брокера provisioner выдаёт private profile; переменная
`SKVOZ_PROFILE` указывает на absolute private filename по [инструкции](../docs/daemon-ipc.md). Запуск:

```sh
./target/release/skvoz-core-daemon --check-config "$SKVOZ_PROFILE"
./target/release/skvoz-core-daemon --config "$SKVOZ_PROFILE"
```

Конфигурация, filesystem policy, startup/shutdown, конечные бюджеты и полный
бинарный контракт находятся в [daemon-ipc.md](../docs/daemon-ipc.md). Daemon
не разбирает HTTP/SOCKS/адрес назначения, не выдаёт credentials/PeerId/сертификаты
и не восстанавливает старые byte streams. Incoming opens получает один локальный
acceptor; разные outbound IPC sessions владеют только собственными потоками.

Публичные [vectors](tests/fixtures/ipc-v1.tsv), [Python](../clients/python/README.md)
и [Ruby](../clients/ruby/README.md) позволяют реализовать другой клиент по контракту.
