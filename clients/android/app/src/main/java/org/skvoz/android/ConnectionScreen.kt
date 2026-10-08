package org.skvoz.android

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalClipboard
import androidx.compose.ui.platform.ClipEntry
import android.content.ClipData
import kotlinx.coroutines.launch
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle

@Composable internal fun ConnectionScreen(model: MainViewModel, onConnect: () -> Unit, onImport: () -> Unit, onVpnSettings: () -> Unit, onBatterySettings: () -> Unit) {
    val editor by model.editor.collectAsStateWithLifecycle()
    val state by model.connection.collectAsStateWithLifecycle()
    val diagnostics by model.diagnostics.collectAsStateWithLifecycle()
    val diagnosticControls by model.diagnosticControls.collectAsStateWithLifecycle()
    val startup by model.startup.collectAsStateWithLifecycle()
    var settingsOpen by remember { mutableStateOf(false) }
    var appsOpen by remember { mutableStateOf(false) }
    var batteryConfirm by remember { mutableStateOf(false) }
    val clipboard = LocalClipboard.current
    val clipboardScope = rememberCoroutineScope()
    MaterialTheme(colorScheme = if (androidx.compose.foundation.isSystemInDarkTheme()) darkColorScheme() else lightColorScheme()) {
        Surface(Modifier.fillMaxSize()) {
            Column(Modifier.safeDrawingPadding().fillMaxSize().verticalScroll(rememberScrollState()).padding(20.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) {
                Text("Соединение SKVOZ", style = MaterialTheme.typography.headlineSmall)
                Card(Modifier.fillMaxWidth()) {
                    Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(4.dp)) {
                        Text(phaseText(state.phase), style = MaterialTheme.typography.titleMedium)
                        Text(if (state.mode == Mode.VPN) "ВПН" else "Прокси", style = MaterialTheme.typography.bodyMedium)
                        if (state.alwaysOn) Text("Always-on включён: соединением управляет Android", style = MaterialTheme.typography.bodySmall)
                        if (state.lockdown) Text("Android блокирует соединения вне ВПН, включая исключённые приложения.", style = MaterialTheme.typography.bodySmall)
                        if (state.error != null) Text(errorText(state.error!!), color = MaterialTheme.colorScheme.error)
                    }
                }
                OutlinedTextField(editor.settings.endpoint, { value -> if (value.length <= 260) model.edit { it.copy(endpoint = value) } },
                    label = { Text("Сервер и порт") }, placeholder = { Text("server.example:4222") }, singleLine = true, enabled = !editor.busy, modifier = Modifier.fillMaxWidth())
                OutlinedTextField(editor.settings.login, { value -> if (value.length <= 64) model.edit { it.copy(login = value) } },
                    label = { Text("Логин") }, singleLine = true, enabled = !editor.busy, modifier = Modifier.fillMaxWidth())
                OutlinedTextField(editor.password, model::password, label = { Text("Пароль") }, singleLine = true,
                    enabled = !editor.busy, visualTransformation = PasswordVisualTransformation(), keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Password, autoCorrectEnabled = false), modifier = Modifier.fillMaxWidth())
                Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                    Mode.entries.forEach { mode -> FilterChip(selected = editor.settings.mode == mode,
                        onClick = { if (!state.alwaysOn) model.edit { it.copy(mode = mode) } }, enabled = !state.alwaysOn && !editor.busy,
                        label = { Text(if (mode == Mode.VPN) "ВПН" else "Прокси") }) }
                }
                if (editor.error != null) Text(errorText(editor.error!!), color = MaterialTheme.colorScheme.error)
                Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                    Button(onClick = if (startup != null) model::stop else if (state.alwaysOn) onVpnSettings else if (state.active) model::stop else onConnect,
                        enabled = editor.loaded && !editor.busy && state.phase != "stopping", modifier = Modifier.weight(1f).heightIn(min = 48.dp)) {
                        Text(if (startup != null) "Отменить" else if (state.alwaysOn) "Настройки ВПН" else if (state.active) "Отключить" else "Подключить")
                    }
                    OutlinedButton(onClick = { settingsOpen = !settingsOpen }, modifier = Modifier.heightIn(min = 48.dp)) { Text("Настройки") }
                }
                if (state.active || state.alwaysOn) TextButton(onClick = onConnect, enabled = state.profileApplyEnabled(editor.loaded, editor.busy, startup != null)) { Text(if (state.alwaysOn) "Сохранить и применить" else "Сохранить и переподключить") }
                Card(Modifier.fillMaxWidth()) {
                    Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(4.dp)) {
                        Text("↑ ${volume(state.upRate)}/с    ↓ ${volume(state.downRate)}/с")
                        Text("Отправлено ${volume(state.uploaded)} · получено ${volume(state.downloaded)}", style = MaterialTheme.typography.bodySmall)
                        Text("Время ${duration(state.elapsed)}", style = MaterialTheme.typography.bodySmall)
                    }
                }
                if (editor.settings.mode == Mode.PROXY) {
                    listOf("http://127.0.0.1:${editor.settings.httpPort}", "socks5://127.0.0.1:${editor.settings.socksPort}").forEach { uri ->
                        Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
                            Text(uri, modifier = Modifier.weight(1f).padding(top = 14.dp), style = MaterialTheme.typography.bodySmall)
                            TextButton(onClick = { clipboardScope.launch { clipboard.setClipEntry(ClipEntry(ClipData.newPlainText("Local endpoint", uri))) } }) { Text("Копировать") }
                        }
                    }
                }
                if (settingsOpen) {
                    HorizontalDivider()
                    Text("Настройки соединения", style = MaterialTheme.typography.titleMedium)
                    PortField("HTTP / CONNECT", editor.settings.httpPort) { port -> model.edit { it.copy(httpPort = port) } }
                    PortField("SOCKS5 TCP", editor.settings.socksPort) { port -> model.edit { it.copy(socksPort = port) } }
                    Text(if (editor.settings.customCa) "TLS: импортированный CA" else "TLS: доверенные центры Android", style = MaterialTheme.typography.bodySmall)
                    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        OutlinedButton(onClick = onImport) { Text("Импорт CA") }
                        if (editor.settings.customCa) TextButton(onClick = { model.edit { it.copy(customCa = false) } }) { Text("CA Android") }
                    }
                    OutlinedButton(onClick = { model.listApps(); appsOpen = true }) { Text("Приложения ВПН (${editor.settings.packages.size})") }
                    Text("Прокси обслуживает только приложения с настроенным локальным HTTP или SOCKS5 адресом. ВПН передаёт IP-трафик выбранных приложений; семейства адресов определяет сервер.", style = MaterialTheme.typography.bodySmall)
                    Text(if (editor.batteryExempt == true) "Android: экономия заряда для SKVOZ отключена" else "Android: экономия заряда может ограничивать сеть при выключенном экране", style = MaterialTheme.typography.bodySmall)
                    TextButton(onClick = { batteryConfirm = true }) { Text("Работа при выключенном экране") }
                    Text("Для непрерывного ручного ВПН Always-on не обязателен. На некоторых телефонах дополнительно разрешите фоновый запуск SKVOZ в настройках производителя.", style = MaterialTheme.typography.bodySmall)
                    TextButton(onClick = onVpnSettings) { Text("Always-on и блокировка вне ВПН") }
                    Text("Эти функции включаются в настройках Android. Блокировка вне ВПН мешает прямой работе исключённых приложений. До разблокировки телефона сохранённый профиль недоступен.", style = MaterialTheme.typography.bodySmall)
                }
                TextButton(onClick = { model.diagnosticPanel(!diagnosticControls.open) }) { Text(if (diagnosticControls.open) "Скрыть диагностику" else "Диагностика") }
                if (diagnosticControls.open) {
                    Card(Modifier.fillMaxWidth()) {
                        Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(4.dp)) {
                            Text("Диагностика сети", style = MaterialTheme.typography.titleMedium)
                            BASIC_METRICS.forEach { key -> diagnostics.basic[key]?.let { Text("${metricLabel(key)}: $it", style = MaterialTheme.typography.bodySmall) } }
                            if (diagnostics.basic.isEmpty()) Text("Нет текущего образца")
                            Row { Switch(diagnosticControls.detailed, model::diagnosticDetail); Text("Подробные измерения", Modifier.padding(top = 14.dp)) }
                            val detail = diagnostics.detail
                            Text(if (!diagnosticControls.detailed) "Подробный сбор выключен" else if (detail == null || detail.age == null) "Ожидание образца" else if (!detail.fresh) "Устаревший образец (${detail.age} мс)" else "Возраст образца: ${detail.age} мс", style = MaterialTheme.typography.bodySmall)
                            if (detail?.enabled == true) DETAILED_METRICS.forEach { key -> detail.values[key]?.let { Text("${metricLabel(key)}: $it", style = MaterialTheme.typography.bodySmall) } }
                            if (diagnostics.code != null) Text("Измерения недоступны; соединение продолжает работать.", style = MaterialTheme.typography.bodySmall)
                            Text("Время — микросекунды ожидания и обработки, не загрузка CPU. Счётчики сбрасываются при новом сборе или соединении. Закрытие экрана выключает подробный сбор.", style = MaterialTheme.typography.bodySmall)
                            TextButton(onClick = { val report = diagnosticReport(state, diagnostics); clipboardScope.launch { clipboard.setClipEntry(ClipEntry(ClipData.newPlainText("SKVOZ diagnostics", report))) } }) { Text("Копировать отчёт") }
                        }
                    }
                }
                HorizontalDivider()
                Text("Журнал", style = MaterialTheme.typography.titleMedium)
                state.journal.takeLast(20).reversed().forEach { entry -> Text("${journalTime(entry.time)}  ${entry.text}", style = MaterialTheme.typography.bodySmall) }
                if (state.journal.isEmpty()) Text("Событий пока нет", style = MaterialTheme.typography.bodySmall)
                Text("Закрытие экрана сохраняет активное соединение. Управление доступно в уведомлении.", style = MaterialTheme.typography.bodySmall)
            }
            if (batteryConfirm) AlertDialog(onDismissRequest = { batteryConfirm = false }, title = { Text("Непрерывное соединение") },
                text = { Text("Для связи при выключенном экране можно разрешить SKVOZ работать без ограничений экономии заряда. Это может увеличить расход батареи. Решение принимает Android; настройки производителя могут требовать отдельного разрешения фонового запуска.") },
                confirmButton = { TextButton(onClick = { batteryConfirm = false; onBatterySettings() }) { Text("Настройки Android") } },
                dismissButton = { TextButton(onClick = { batteryConfirm = false }) { Text("Позже") } })
            if (appsOpen) AppSelection(editor, model, onDismiss = { appsOpen = false })
        }
    }
}
@Composable private fun PortField(label: String, port: Int, change: (Int) -> Unit) {
    var text by remember(port) { mutableStateOf(port.toString()) }
    OutlinedTextField(text, { value -> if (value.length <= 5 && value.all(Char::isDigit)) { text = value; value.toIntOrNull()?.let(change) } },
        label = { Text("Порт $label") }, singleLine = true, modifier = Modifier.fillMaxWidth())
}
@Composable private fun AppSelection(editor: Editor, model: MainViewModel, onDismiss: () -> Unit) {
    var search by remember { mutableStateOf("") }
    AlertDialog(onDismissRequest = onDismiss, title = { Text("Приложения ВПН") }, confirmButton = { TextButton(onClick = onDismiss) { Text("Готово") } },
        text = { Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
            AppPolicy.entries.forEach { policy ->
                Row { RadioButton(selected = editor.settings.appPolicy == policy, onClick = { model.edit { it.copy(appPolicy = policy) } })
                    Text(if (policy == AppPolicy.INCLUDE) "Только выбранные" else "Все, кроме выбранных", Modifier.padding(top = 14.dp)) }
            }
            if (editor.settings.appPolicy == AppPolicy.INCLUDE && editor.settings.packages.isEmpty()) Text("Выберите хотя бы одно приложение.", color = MaterialTheme.colorScheme.error)
            Text("SKVOZ всегда исключён. Список остаётся на телефоне.", style = MaterialTheme.typography.bodySmall)
            OutlinedTextField(search, { search = it.take(256) }, label = { Text("Поиск") }, singleLine = true)
            LazyColumn(Modifier.heightIn(max = 340.dp)) {
                items(editor.apps.filter { it.name.contains(search, true) || it.label.contains(search, true) }, key = { it.name }) { app ->
                    Row(Modifier.fillMaxWidth()) {
                        Checkbox(app.name in editor.settings.packages, { model.selected(app.name, it) })
                        Column(Modifier.weight(1f).padding(top = 8.dp)) {
                            Text(app.label, style = MaterialTheme.typography.bodyMedium)
                            Text(if (app.missing) "Удалено: снимите выбор" else app.name, style = MaterialTheme.typography.bodySmall)
                        }
                    }
                }
            }
            Text("Изменения применяются кнопкой «Подключить» или «Сохранить и переподключить».", style = MaterialTheme.typography.bodySmall)
        } })
}
private fun journalTime(time: Long): String = java.text.SimpleDateFormat("dd.MM HH:mm:ss", java.util.Locale.getDefault()).format(java.util.Date(time))
internal fun volume(bytes: Long): String = when {
    bytes >= 1073741824 -> "%.1f ГиБ".format(bytes / 1073741824.0)
    bytes >= 1048576 -> "%.1f МиБ".format(bytes / 1048576.0)
    bytes >= 1024 -> "%.1f КиБ".format(bytes / 1024.0)
    else -> "$bytes Б"
}
internal fun duration(milliseconds: Long): String {
    val seconds = milliseconds.coerceAtLeast(0) / 1000
    return "%02d:%02d:%02d".format(seconds / 3600, seconds / 60 % 60, seconds % 60)
}
internal fun errorText(code: String): String = when (code) {
    "battery_settings_unavailable" -> "На этом телефоне настройка недоступна. Откройте сведения о приложении SKVOZ и параметры батареи."
    "invalid_address" -> "Укажите DNS или IP и внешний порт; IPv6 — в квадратных скобках."
    "invalid_login" -> "Логин: до 64 латинских букв, цифр, _ или -."
    "invalid_password" -> "Пароль должен содержать от 12 до 72 байт UTF-8."
    "profile_incomplete" -> "Заполните сервер, логин и пароль."
    "authentication_failed" -> "Сервер отклонил логин или пароль."
    "certificate_failed" -> "Проверка сертификата сервера не пройдена."
    "server_unavailable", "network_unavailable" -> "Сеть или сервер недоступны."
    "version_mismatch", "unsupported_version" -> "Версии клиента и сервера несовместимы."
    "empty_app_selection" -> "Для режима «Только выбранные» нужно выбрать приложение."
    "selected_app_missing" -> "Выбранное приложение удалено. Обновите список."
    "foreground_start_denied" -> "Android запретил запуск фонового соединения. Проверьте разрешения и настройки ВПН."
    "vpn_permission_required" -> "Нужно разрешение Android на ВПН."
    "notifications_denied" -> "Уведомления запрещены. Соединение видно в диспетчере активных приложений Android."
    "credential_key_missing", "credential_decrypt_failed", "credential_corrupt" -> "Сохранённый пароль не удалось расшифровать. Очистите данные приложения и настройте профиль заново."
    "settings_read_failed", "unsupported_settings", "invalid_settings" -> "Сохранённые настройки недоступны или несовместимы."
    "local_setup_failed" -> "Не удалось открыть локальные порты или интерфейс."
    else -> "Не удалось выполнить операцию ($code)."
}

internal fun metricLabel(key: String): String = when (key) {
    "packet_in" -> "IP-пакеты от приложений"
    "packet_out" -> "IP-пакеты к приложениям"
    "packet_dropped" -> "Отброшено IP-пакетов"
    "queue_bytes" -> "IP-очереди, байты"
    "queue_records" -> "IP-очереди, записи"
    "buffer_bytes" -> "Зарезервировано памяти, байты"
    "buffer_records" -> "Зарезервировано записей"
    "collection" -> "Номер сбора в текущем runtime"
    "samples" -> "Образцы"
    "elapsed_ms" -> "Время сбора, мс"
    "turns" -> "Циклы локального I/O"
    "native_us" -> "Локальный I/O, мкс"
    "drive_us" -> "Обработка сети и ожидания, мкс"
    "read_full" -> "Полные серии чтения TUN (16)"
    "write_full" -> "Полные серии записи TUN (16)"
    "read_block" -> "TUN: чтение WouldBlock"
    "write_block" -> "TUN: запись WouldBlock"
    "read_paused" -> "TUN: паузы при заполнении очереди"
    "core_turns" -> "Циклы готового Core"
    "core_turn_us" -> "Core: обработка и ожидания, мкс"
    "core_progress" -> "Core: работа до idle"
    "core_idle_count" -> "Core: входы в ожидание"
    "core_idle_us" -> "Core: ожидание и приём, мкс"
    "core_output_us" -> "Core: вывод, мкс"
    else -> key
}
