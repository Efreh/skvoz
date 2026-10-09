package org.skvoz.android.ui

import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.Alignment
import androidx.compose.foundation.layout.*
import androidx.compose.ui.unit.dp
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import org.skvoz.android.*

@Composable internal fun SettingsScreen(editor: Editor, state: ConnectionState,
    proxy: () -> Unit, apps: () -> Unit, trust: () -> Unit, vpn: () -> Unit, battery: () -> Unit) {
    var batteryDialog by remember { mutableStateOf(false) }
    FormPage {
        if (editor.dirty) Notice("Изменения не применены")
        SectionTitle("Подключение")
        TaskRow("Настройки прокси", "HTTP ${editor.httpPort} · SOCKS5 ${editor.socksPort}", Glyph.SETTINGS, proxy)
        TaskRow("Приложения ВПН", policySummary(editor.settings.appPolicy, editor.settings.packages.size), Glyph.APPS, apps)
        TaskRow("Доверие TLS", if (editor.settings.customCa) "Импортированный CA" else "Доверенные центры Android", Glyph.SHIELD, trust)
        SectionTitle("Работа в фоне")
        TaskRow("При выключенном экране", when (editor.batteryExempt) {
            true -> "Экономия заряда для SKVOZ отключена"; false -> "Android может ограничивать сеть"; null -> "Статус экономии заряда неизвестен"
        }, Glyph.POWER, { batteryDialog = true })
        TaskRow("Системные настройки ВПН", when {
            state.lockdown -> "Always-on · блокировка вне ВПН включена"
            state.alwaysOn -> "Always-on включён"
            else -> "Always-on и блокировка вне ВПН — в Android"
        }, Glyph.SHIELD, vpn)
        Hint("Для ручного ВПН Always-on не обязателен. Блокировка вне ВПН может мешать прямой работе исключённых приложений.")
        EditorError(editor)
        Hint("SKVOZ ${BuildConfig.VERSION_NAME}")
    }
    if (batteryDialog) AlertDialog(onDismissRequest = { batteryDialog = false }, title = { Text("Работа при выключенном экране") },
        text = { Text("Android может ограничивать сеть в режиме экономии заряда. Можно разрешить SKVOZ работу без этих ограничений. Это может увеличить расход батареи и не гарантирует сохранение процесса. На некоторых телефонах требуется также разрешение фонового запуска в настройках производителя.", Modifier.verticalScroll(rememberScrollState())) },
        confirmButton = { TextButton({ batteryDialog = false; battery() }) { Text("Настройки Android") } },
        dismissButton = { TextButton({ batteryDialog = false }) { Text("Позже") } })
}

@Composable internal fun TrustScreen(editor: Editor, startup: Boolean, import: () -> Unit, androidCa: () -> Unit) {
    FormPage {
        Panel {
            Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                Symbol(Glyph.SHIELD, color = SkvozColors.Accent)
                Column(Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(4.dp)) {
                    Text(if (editor.settings.customCa) "Импортированный CA" else "Доверенные центры Android", style = MaterialTheme.typography.titleSmall)
                    Hint("Выбрано для следующего подключения")
                }
            }
            if (editor.settings.customCa != editor.savedSettings.customCa)
                StatLine("Сохранённое доверие", if (editor.savedSettings.customCa) "Импортированный CA" else "CA Android")
        }
        SectionTitle("Проверка TLS обязательна")
        Hint("Проверяются сертификат и имя или IP сервера. Отключение проверки не предусмотрено.")
        if (editor.settings.customCa != editor.savedSettings.customCa) Notice("Изменения не применены. Подключите или переподключите профиль для применения CA Android.")
        PrimaryButton(if (editor.busy) "Импорт…" else "Импорт CA", import, !editor.busy && !startup)
        SecondaryButton("CA Android", androidCa, editor.settings.customCa && !editor.busy && !startup)
        Hint("Импорт PEM/DER сразу сохраняет доверенный CA для следующей попытки, включая автоматическое восстановление. Текущее соединение не перезапускается. Для немедленного применения переподключите профиль.")
        Hint("CA Android применяется вместе с остальными изменениями профиля. Отмена выбора файла оставляет прежнее доверие.")
        EditorError(editor)
    }
}
