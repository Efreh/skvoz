# Клиент Android

«Соединение SKVOZ» 1.1.0 — клиент Android API31+ с двумя режимами: **Прокси**
и **ВПН**. APK включает Kotlin/Compose/Material3 и ту же Rust-библиотеку Core4.0.1
с общим network runtime0.4.2. Application ID — `org.skvoz.android`;
ABI — `arm64-v8a` и `x86_64`. Сервер остаётся общим для любых совместимых клиентов.

Исходники, crossbuild двух ABI и Linux JVM/JNI transport checks доступны.
Работа `VpnService`, маршрутизация приложений, фон, Android Keystore и поведение
Android16/16KiB требуют проверки на устройстве; сборка APK сама по себе их не доказывает.

## Подключение

1. Установите подписанный APK из Android Releases. Проверьте файл `SHA256SUMS`.
   Для обновлений требуется тот же ключ подписи; test/debug APK имеет другую
   подпись и не обновляет официальный APK без удаления предыдущей установки.
2. Введите DNS или IP сервера с внешним портом (`server.example:4222`,
   для IPv6 — `[2001:db8::1]:4222`), логин и пароль. Идентификатор устройства
   выделяется автоматически; `peer_id` вручную вводить не нужно.
3. Выберите режим и нажмите **Подключить**. ВПН требует отдельного системного
   разрешения Android. Запрет уведомлений не отменяет пользовательский запуск;
   состояние service также видно в системном диспетчере активных приложений.
4. Для частного центра сертификации откройте **Настройки → Импорт CA** и выберите
   PEM/DER сертификаты trusted CA. Для сервера с публичным CA импорт не нужен:
   приложение использует доверенные центры Android. Проверка имени/IP и TLS
   обязательна, порядок NATS — INFO → TLS → CONNECT; небезопасного fallback нет.

Логин содержит1–64 ASCII букв/цифр/`_`/`-`; зарезервированный серверный логин
не допускается. Пароль —12–72 байта UTF-8. Пароль сохраняется ciphertext в private
DataStore, AES-GCM key создаётся в Android Keystore на телефоне. Этот key не
является ключом подписи APK. Профиль исключён из backup; потеря key/повреждение
настроек — явная ошибка. Для повторной настройки после потери key очистите данные
приложения в Android settings. Перед разблокировкой телефона профиль недоступен.

## Прокси и ВПН

В режиме Прокси доступны loopback URI `http://127.0.0.1:18080` и
`socks5://127.0.0.1:18081`. Настройте выбранное приложение на нужную URI; кнопка
**Копировать** копирует её целиком. Порты изменяются в настройках и должны быть
разными, свободными и в диапазоне1–65535. HTTP forwarding, HTTPS CONNECT и SOCKS5
CONNECT передают TCP bytes с half-close. SOCKS5 UDP ASSOCIATE не поддерживается.
Приложение не меняет системные proxy settings.

В режиме ВПН `VpnService` создаёт nonblocking TUN и передаёт owned duplicate FD
одному Rust actor. Пакеты TCP/UDP/DNS/ICMP идут через общий L3 contract; Kotlin
не читает и не пересылает payload. Сервер согласует IPv4/IPv6, grants, маршруты,
DNS и MTU; неподдержанный путь отклоняется без прямого обхода. Устройство должно
иметь Android VPN permission; root не нужен.

В **Приложения ВПН** доступны взаимоисключающие варианты:

- **Только выбранные** — нужно выбрать хотя бы одно установленное приложение.
- **Все, кроме выбранных** — выбранные приложения исключаются.

Список включает приложения без launcher activity, хранится локально и не
передаётся серверу. SKVOZ всегда исключён, чтобы все native NATS connections
оставались вне собственного TUN. Удалённый выбранный package требует исправления
списка; он не игнорируется молча. Изменения сохраняются и применяются кнопкой
**Сохранить и переподключить**; старые flows завершаются, создаётся один новый
runtime/TUN. При Always-on эта кнопка называется **Сохранить и применить**.

## Фон, восстановление и системные настройки

Закрытие/поворот Activity не отключает соединение. Foreground notification показывает
состояние и позволяет отключить ручное соединение. После потери сети или изменения
маршрута клиент повторяет подключение с конечной задержкой1–15s; новые соединения
работают после нового enrollment/join, старые bytes не возобновляются. Ошибки
TLS/auth/version/configuration прекращают попытку и требуют исправления причины.
Закрытие native transport повторяет подключение; ошибка очистки не заменяет
причину исходного отключения.

По умолчанию запуск ручной. **Always-on** и **Block connections without VPN**
включаются пользователем в системных VPN settings. Собственного KillSwitch,
BOOT receiver и autostart option нет. Always-on восстанавливает сохранённый VPN
профиль; переключение в Прокси недоступно. Отключение управляется системными settings,
куда ведут главный action и notification. Изменение профиля того же ВПН допускается.
Lockdown может блокировать прямую работу исключённых приложений — учитывайте это
при выборе списка. Абсолютная гарантия отсутствия утечек не заявляется.

Для ручного ВПН Always-on не обязателен. В **Настройки → Работа при выключенном
экране** показан статус экономии заряда; после пояснения пользователь может
открыть системный запрос отключения ограничений для SKVOZ. Foreground service
не освобождает приложение от сетевых ограничений Doze. На HONOR и других
устройствах могут понадобиться отдельные разрешения фонового запуска в настройках
производителя. Эти настройки не меняются автоматически; отключение экономии
заряда может увеличить расход батареи и не гарантирует сохранение процесса.
[Ограничения Doze](https://developer.android.com/training/monitoring-device-state/doze-standby).

Скриншоты приложения разрешены; поле пароля по-прежнему скрывает введённый текст.

Главный экран показывает реальные upload/download counters, rates и elapsed time
по monotonic clock. Журнал сохраняет до200 событий в private DataStore (до32KiB),
включая запуск процесса, службы, переподключение и отзыв ВПН. Он содержит только
известные коды без raw configs, паролей, payload и CA contents. После перезапуска
доступны последние события; время записи — дата и часы телефона, поэтому смена
часов может изменить порядок отображаемого времени. На экране видны последние20
записей. При внезапном завершении процесса последняя ещё не записанная запись
может отсутствовать.

## Диагностика сети

Кнопка «Диагностика» раскрывает локальные IP-счётчики, очереди и зарезервированную
память из существующих STATS. «Подробные измерения» по умолчанию выключены и
не сохраняются между запусками. Сбор действует только на открытом экране с
раскрытой панелью; закрытие панели или уход с экрана выключает его, сохраняя ВПН.
Образцы обновляются не чаще раза в секунду; UI отмечает ожидание и возраст,
после3 секунд — устаревший образец. Ошибка диагностики не отключает соединение.

Подробные счётчики накапливаются за один сбор текущего runtime и сбрасываются
при выключении/повторном включении или переподключении. Внутренний Core recovery
того же объекта не сбрасывает их; Core timing относится к Ready turns.
Времена обработки/вывода/idle измеряются в микросекундах с ожиданиями, не как CPU
load. Idle включает приём после ожидания; интервалы вложены и не складываются.
Число полных серий TUN/WouldBlock и очередь помогают различать локальное
backpressure и ожидание транспорта, но не доказывают причину ограничения скорости.

«Копировать отчёт» передаёт только числовые поля, известные коды состояния и
версии компонентов. Сервер, логин, пароль, устройство, приложения, CA, пакеты и
raw logcat не включаются. Отчёт остаётся локальным до действия пользователя.
Сбор не меняет сеть, Core wire, windows или budgets; конкретные расходы
подробного режима на телефоне требуют отдельного измерения.

## Сборка и проверки

Требуются Linux build host, JDK17, Rust1.92.0 с targets `aarch64-linux-android` и
`x86_64-linux-android`, Android SDK platform37.0 revision2, build-tools37.0.0,
NDK30.0.16248370. Gradle wrapper9.6.0 проверяет distribution SHA256, AGP9.4.1
использует built-in Kotlin2.2.10; Compose compiler и serialization plugin также2.2.10,
Compose BOM2026.09.00, coroutines и serialization runtime1.11.0. `ANDROID_HOME` указывает на SDK.

```sh
rustup target add --toolchain 1.92.0 aarch64-linux-android x86_64-linux-android
cd clients/android
./gradlew --no-daemon --max-workers=1 assembleDebug assembleDebugAndroidTest testDebugUnitTest lintDebug
python3 tools/check-apk.py app/build/outputs/apk/debug/app-debug.apk --build-tools "$ANDROID_HOME/build-tools/37.0.0"
```

`preBuild` собирает две `.so` через `tools/build-native.py`, Cargo `--locked`,
NDK clang/API31 и linker page alignment16384. Native inputs включают все actual
shared sources и Cargo manifests/lock; изменения Rust инвалидируют Gradle task.
APK содержит общий JNI и служебные native libraries AndroidX Graphics/DataStore.
Все библиотеки хранятся без сжатия; checker проверяет их ABI/ELF LOAD, stack,
zipalign16KiB и подпись. Это совместимость артефакта, а не проверка реального16KiB kernel.

JVM tests проверяют bounded readers/settings/cancellation/generation isolation.
`app/src/androidTest` содержит отдельные tests настоящего Android Keystore и Compose;
запустите `./gradlew connectedDebugAndroidTest` с подключённым Android31+ устройством.
Для реального JNI/TLS/NATS regression используйте `spec/native_spec.rb` с dependency
environment серверных RSpec и Linux release `.so`/runtime; fixture server требует
UID/GID10001, caps0 и no-new-privileges, как [сервер](server-connector.md).

## Android Releases

Собственный workflow проверяет pull requests без signing Secrets. Успешные
main/master builds и точный tag `android-v1.1.0` публикуют официальный signed APK
и SHA256SUMS; main/master получают `android-v1.1.0-build.<run_number>`. APK versionCode
равен1000000+номер выполнения Android workflow; повторное выполнение сохраняет
тот же code. Не сбрасывайте workflow counter при выпуске обновлений.

Для подписи нужны только два GitHub Secrets: `ANDROID_KEYSTORE_B64` — keystore
в Base64, `ANDROID_KEYSTORE_PASSWORD` — его пароль. Alias ключа фиксирован:
`skvoz-android`; пароль ключа должен совпадать с паролем keystore.
Сохраните постоянный приватный keystore вне Git и резервную копию отдельно.
Создать совместимый JKS можно следующей командой; при запросе пароля ключа
нажмите Enter, чтобы использовать пароль keystore:

```sh
umask 077
mkdir -p "$HOME/.local/share/skvoz"
chmod 700 "$HOME/.local/share/skvoz"
keytool -genkeypair -keystore "$HOME/.local/share/skvoz/skvoz-android-release.jks" -storetype JKS -alias skvoz-android -keyalg RSA -keysize 3072 -validity 10000
base64 -w 0 "$HOME/.local/share/skvoz/skvoz-android-release.jks"
```

Release получает DER сертификат alias через `keytool -exportcert` и сравнивает
его SHA256 со всеми сертификатами APK через `tools/check-apk.py --certificate-sha256`.
Это сверка с keystore из Secret; отдельного независимого fingerprint нет.
Отсутствующий или неверный keystore, пароль, alias либо несовпадающая подпись
останавливают release; debug key не используется для официальной публикации.
Временные keystore и DER удаляются и при ошибке.
APK сначала проверяется, затем загружается в draft Release вместе с checksum,
после чего draft становится published. Существующие Releases не заменяются и не удаляются.
Удалённое выполнение workflow/signing/publishing требует отдельной проверки.

## Ручная проверка APK

На отдельном тестовом профиле Android16:

1. Проверьте bad password/CA/name и успешный connect, restart/disconnect/reopen с
   сохранённым паролем, busy ports, HTTP/CONNECT/SOCKS и двунаправленный binary transfer.
2. ВПН: TCP/UDP/DNS/ICMP и QUIC к разрешённым server destinations; IPv4/IPv6 и явный
   unsupported-family/config rejection. Сверьте внешний адрес и DNS с server policy.
3. Для двух приложений A/B проверьте INCLUDE/EXCLUDE, empty allow, удалённый package,
   изменение выбора при active session. SKVOZ joins/new lanes/reconnect не попадают в TUN.
4. Закройте/поверните/reopen экран, запретите notification, выключите экран,
   переключите Wi-Fi/mobile, временно остановите broker. Новые flows восстановятся,
   старые завершатся; counters/journal остаются корректными. Проверьте revoke/system stop.
5. Включите Always-on и lockdown в Android settings, перезапустите app/device,
   примените новый app list, выключите Always-on через settings. Отдельно проверьте
   поведение до/после unlock и конфликт исключённых приложений с lockdown.
6. После warmup выполните50 connect/stop cycles: нет второго runtime, FD возвращаются
   к baseline, память не растёт монотонно. Измерьте idle/active CPU/PSS и battery
   с указанием модели/OS/page size, workload и времени. Проверьте min31 и16KiB отдельно.

Запишите модель, Android/API, page size, server/client versions, фактический результат
каждого сценария и ограничения. Linux regression или успешная сборка не заменяют эти проверки.
