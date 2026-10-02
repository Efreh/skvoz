# Python IPC example

`skvoz_ipc.py` использует только Python3.9+ standard library и локальный
[IPC v1](../../docs/daemon-ipc.md). Это небольшой bounded protocol helper, не
готовый proxy, SDK с reconnect или автоматическое provisioning.

После запуска двух daemon instances и принимающего echo host из собственного
приложения:

```sh
SKVOZ_SOCKET="/absolute/private-directory/core.sock"
printf 'hello' | python3 clients/python/echo.py --socket "$SKVOZ_SOCKET" --peer 0
```

Подставьте собственный absolute pathname в `SKVOZ_SOCKET`.

`echo.py` отправляет до65536 binary stdin bytes и печатает reverse reply;
пустой input поддерживается. JOIN обеспечивает готовую session, SEND оставляет
непринятый suffix, FINISH допускает reply после EOF. CONSUME вызывается после
записи output. Ранний terminal reply при неполных bytes вызывает ошибку.
Helper ограничивает retained events4096 slots/8MiB; приложение должно их дренировать.

Полностью воспроизводимый пример с provisioned identities, принимающим Python
host и реальным брокером запускается из корня:

```sh
python3 testbench/run.py daemon
```

Получение DATA не возвращает credit. `consume` возвращает4 при уже terminal
handle: это cleanup, а не подтверждение обработки. Новый IPC session не получает
старые handles/data. Приложение само выбирает metadata, incoming accept/reject и
должно соблюдать budgets/consumption из канонического контракта.
