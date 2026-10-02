# Клиенты SKVOZ

Каталог клиентских подпроектов общего репозитория SKVOZ.
Готовые клиентские приложения пока не созданы. [Python](python/README.md) и
[Ruby](ruby/README.md) содержат standard-library helpers и binary echo examples
для единого [daemon/IPC v1](../docs/daemon-ipc.md), без native bindings.

Каждый клиент размещается в `clients/<name>/` и хранит там свои исходники,
manifest/build config, тесты, README и необходимые ресурсы. Язык и
платформенные инструменты определяются для конкретного клиента отдельно.
Rust-клиенты включаются в корневой workspace; другие языки используют свои
инструменты внутри подпроекта.

Клиенты подключают [ядро](../core/README.md) через согласованную границу,
а не копируют его implementation. Standalone daemon/IPC реализован для Linux; native FFI и платформенные
адаптеры остаются будущей работой.
Тестовые buffer connectors сейчас находятся в [стенде](../testbench/README.md).

Общие правила: [структура репозитория](../docs/repository.md).
