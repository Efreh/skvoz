# Ruby IPC example

`skvoz_ipc.rb` использует только Ruby3.4+ standard library (`socket`) и единый
[IPC v1](../../docs/daemon-ipc.md). Native extension или отдельная реализация
Core не требуется. Это bounded example helper, не полный proxy/reconnect SDK.

С работающим принимающим echo host:

```sh
SKVOZ_SOCKET="/absolute/private-directory/core.sock"
printf 'hello' | ruby clients/ruby/echo.rb --socket "$SKVOZ_SOCKET" --peer 0
```

Подставьте собственный absolute pathname в `SKVOZ_SOCKET`.

Example поддерживает0..65536 binary stdin bytes, частичный SEND, FINISH и reply
после EOF. Раннее завершение до полного echo вызывает ошибку. Helper проверяет
version/kinds/request IDs и ограничивает retained events4096 slots/8MiB;
приложение должно дренировать события и возвращать credit после потребления.
Terminal `consume` может вернуть4: handle уже освобождён, acknowledgment не применён.

Из корня репозитория полная независимая Ruby/Python qualification с временным
INFO → TLS NATS и provisioned IDs:

```sh
python3 testbench/run.py daemon
```

Принимающую metadata/destination policy и accept/reject реализует приложение.
Новый IPC session не восстанавливает прежние streams/handles/data.
