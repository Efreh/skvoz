package org.skvoz.android.ui

import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.material3.*
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.semantics.*
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import org.skvoz.android.*

@Composable internal fun HomeScreen(
    editor: Editor, state: ConnectionState, startup: PendingStartup?, ranges: DisplayRanges,
    submit: () -> Unit, stop: () -> Unit, vpnSettings: () -> Unit,
    profile: () -> Unit, applications: () -> Unit, mode: (Mode) -> Unit, copy: (String) -> Unit, trust: () -> Unit,
    speedFormat: SpeedFormat = SpeedFormat.BYTES, showResources: Boolean = false,
    resources: ResourceState = ResourceState(), resourceRanges: ResourceRanges = ResourceRanges(),
) {
    val fontScale = LocalDensity.current.fontScale
    val error = editor.error ?: state.error
    BoxWithConstraints(Modifier.fillMaxSize()) {
        val wide = maxWidth >= 840.dp && maxHeight >= 480.dp && fontScale < 1.5f
        Row(Modifier.fillMaxSize().padding(horizontal = if (wide) 24.dp else 0.dp), horizontalArrangement = Arrangement.spacedBy(24.dp)) {
            Box(Modifier.weight(1f), contentAlignment = Alignment.TopCenter) {
                Column(Modifier.widthIn(max = 560.dp).fillMaxWidth().verticalScroll(rememberScrollState()).padding(16.dp),
                    verticalArrangement = Arrangement.spacedBy(12.dp)) {
                    val display = if (state.active || state.alwaysOn) state.display else editor.settings.displaySnapshot()
                    Surface(Modifier.fillMaxWidth(), color = Color.Transparent, shape = MaterialTheme.shapes.medium, border = BorderStroke(1.dp, SkvozColors.Line)) {
                        Row(Modifier.panelTone().clickable(onClick = profile).heightIn(min = SkvozLayout.SummaryHeight)
                            .padding(horizontal = SkvozLayout.PanelPadding, vertical = SkvozLayout.ContentGap), verticalAlignment = Alignment.CenterVertically,
                            horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                            Symbol(Glyph.SERVER, color = SkvozColors.Text)
                            Column(Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(4.dp)) {
                                Text(display?.endpoint?.ifEmpty { "Настроить подключение" } ?: "Подготовка", style = MaterialTheme.typography.titleSmall)
                                if (!display?.login.isNullOrEmpty()) Hint("${display!!.login} · TLS")
                            }
                            Symbol(Glyph.NEXT)
                        }
                    }
                    ModeChoice(editor.settings.mode, mode, !editor.busy && startup == null && state.phase != "stopping", state.alwaysOn)
                    if (state.active && editor.settings.mode != state.mode) Hint("Сейчас: ${modeName(state.mode)}. Выбран режим следующего подключения.", warning = true)
                    Panel {
                        Column(Modifier.fillMaxWidth().semantics(mergeDescendants = true) { liveRegion = LiveRegionMode.Polite },
                            verticalArrangement = Arrangement.spacedBy(SkvozLayout.ContentGap)) {
                        Row(verticalAlignment = Alignment.CenterVertically,
                            horizontalArrangement = Arrangement.spacedBy(8.dp, Alignment.CenterHorizontally),
                            modifier = Modifier.fillMaxWidth()) {
                            if (startup != null || state.phase in setOf("preparing", "enrolling", "connecting", "reconnecting", "stopping"))
                                CircularProgressIndicator(Modifier.size(24.dp), strokeWidth = 2.dp)
                            else if (state.phase == "connected") Surface(Modifier.size(32.dp), color = SkvozColors.Accent, shape = CircleShape) {
                                Box(contentAlignment = Alignment.Center) { Symbol(Glyph.CHECK, color = SkvozColors.OnAccent) }
                            } else if (state.phase == "error") Surface(Modifier.size(32.dp), color = SkvozColors.Error, shape = CircleShape) {
                                Box(contentAlignment = Alignment.Center) { Symbol(Glyph.ERROR, color = SkvozColors.OnAccent) }
                            } else Symbol(Glyph.POWER, color = SkvozColors.Accent)
                            Text(if (editor.busy) "Сохранение профиля" else if (startup != null) "Ожидание разрешения Android" else phaseText(state.phase),
                                style = MaterialTheme.typography.headlineSmall, textAlign = TextAlign.Center)
                        }
                        Box(Modifier.fillMaxWidth(), contentAlignment = Alignment.Center) {
                            Text(if (state.phase == "connected") modeName(state.mode) else if (state.phase == "reconnecting") "Восстановление соединения автоматически" else "TLS · SKVOZ",
                                Modifier.fillMaxWidth(), color = SkvozColors.Secondary, style = MaterialTheme.typography.bodySmall, textAlign = TextAlign.Center)
                        }
                        }
                        if (state.phase == "error" && error != null) {
                            Text(errorText(error), Modifier.fillMaxWidth(), color = SkvozColors.Error,
                                style = MaterialTheme.typography.bodyMedium, textAlign = TextAlign.Center)
                            RecoveryAction(error, profile, trust, applications)
                        }
                        val action = connectionAction(state, startup != null, editor.busy)
                        SecondaryButton(when (action) {
                            ConnectionAction.CONNECT -> "Подключить"; ConnectionAction.CANCEL -> "Отменить"
                            ConnectionAction.DISCONNECT -> "Отключить"; ConnectionAction.VPN_SETTINGS -> "Настройки ВПН"; ConnectionAction.WAIT -> if (editor.busy) "Сохранение…" else "Отключение…"
                        }, when (action) { ConnectionAction.CONNECT -> submit; ConnectionAction.VPN_SETTINGS -> vpnSettings; else -> stop }, action != ConnectionAction.WAIT,
                            if (action == ConnectionAction.CANCEL || action == ConnectionAction.DISCONNECT) SkvozColors.Error else SkvozColors.Accent)
                    }
                    if (error != null && state.phase != "error") {
                        Notice(errorText(error), error = error != "notifications_denied")
                        RecoveryAction(error, profile, trust, applications)
                    }
                    if (editor.dirty) {
                        Notice("Изменения не применены\nСледующее подключение: ${editor.settings.endpoint.ifEmpty { "сервер не указан" }} · ${modeName(editor.settings.mode)}")
                        if (state.active || state.alwaysOn) SecondaryButton(applyLabel(state), submit,
                            state.profileApplyEnabled(editor.loaded, editor.busy, startup != null))
                    }
                    if (state.mode == Mode.PROXY && state.phase == "connected") state.display?.let { active ->
                        Panel {
                            SectionTitle("Локальные адреса")
                            active.httpUri?.let { UriRow(it, copy) }; active.socksUri?.let { UriRow(it, copy) }
                            Hint("Работают только приложения, в которых настроен этот прокси.")
                        }
                    }
                    Column(Modifier.fillMaxWidth(), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                        Hint("Текущая скорость")
                        Meter("Получение", if (state.phase == "connected") state.downRate.coerceAtLeast(0).toULong() else 0uL, ranges.rate, true, Glyph.DOWN, speedFormat)
                        Meter("Отправка", if (state.phase == "connected") state.upRate.coerceAtLeast(0).toULong() else 0uL, ranges.rate, true, Glyph.UP, speedFormat)
                    }
                    Column(Modifier.fillMaxWidth(), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                        if (!state.active && state.started > 0) Hint("Последняя сессия")
                        val totals = listOf("Получено" to volume(state.downloaded), "Отправлено" to volume(state.uploaded), "Время" to duration(state.elapsed))
                        if (fontScale >= 1.5f) totals.forEach { (label, value) -> StatLine(label, value) }
                        else Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                            totals.forEachIndexed { index, (label, value) -> Column(Modifier.weight(1f).semantics(mergeDescendants = true) {}) {
                                Hint(label)
                                Text(value, style = MaterialTheme.typography.titleSmall)
                            }
                                if (index < totals.lastIndex) VerticalDivider(Modifier.height(36.dp), color = SkvozColors.Line)
                            }
                        }
                    }
                    if (showResources) ResourceMeters(resources, resourceRanges)
                    if ((display?.mode ?: editor.settings.mode) == Mode.VPN || editor.settings.mode == Mode.VPN) {
                        val policy = display?.appPolicy ?: editor.settings.appPolicy
                        val count = display?.packageCount ?: editor.settings.packages.size
                        TaskRow("Приложения ВПН", policySummary(policy, count), Glyph.APPS, applications)
                        if (state.lockdown) Hint("Блокировка вне ВПН включена: исключённые приложения блокируются Android.", warning = true)
                    }
                    Hint("Закрытие экрана сохраняет соединение. Управление доступно в уведомлении.")
                }
            }
            if (wide) Column(Modifier.weight(0.8f).verticalScroll(rememberScrollState()).padding(vertical = 16.dp)) { JournalPanel(state.journal) }
        }
    }
}

@Composable private fun RecoveryAction(error: String, profile: () -> Unit, trust: () -> Unit, applications: () -> Unit) {
    when (error) {
        "authentication_failed", "profile_validation_failed", "invalid_address", "invalid_login", "invalid_password" -> SecondaryButton("Настроить подключение", profile, color = SkvozColors.Warning)
        "certificate_failed", "invalid_ca", "trusted_ca_unavailable" -> SecondaryButton("Доверие TLS", trust, color = SkvozColors.Warning)
        "selected_app_missing", "empty_app_selection" -> SecondaryButton("Приложения ВПН", applications, color = SkvozColors.Warning)
    }
}

internal fun modeName(mode: Mode) = if (mode == Mode.VPN) "ВПН" else "Прокси"
internal fun policySummary(policy: AppPolicy, count: Int) =
    if (policy == AppPolicy.INCLUDE) "Только выбранные · $count" else if (count == 0) "Все приложения, кроме SKVOZ" else "Все, кроме выбранных · $count"
@Composable internal fun StatLine(label: String, value: String) {
    Row(Modifier.fillMaxWidth().semantics(mergeDescendants = true) {}, horizontalArrangement = Arrangement.spacedBy(12.dp), verticalAlignment = Alignment.Top) {
        Text(label, Modifier.weight(1f), color = SkvozColors.Secondary, style = MaterialTheme.typography.bodyMedium)
        Text(value, style = MaterialTheme.typography.bodyMedium)
    }
}

@Composable internal fun ResourceMeters(resources: ResourceState, ranges: ResourceRanges) {
    Column(Modifier.fillMaxWidth(), verticalArrangement = Arrangement.spacedBy(8.dp)) {
        Hint("Ресурсы приложения")
        Meter("CPU", cpuBasisPoints(resources.cpu), ranges.cpu, glyph = Glyph.CPU,
            valueLabel = cpuLabel(resources.cpu), rangeLabel = cpuLabel(ranges.cpu.toDouble() / 100))
        Meter("RAM (PSS)", (resources.pssBytes ?: 0).coerceAtLeast(0).toULong(), ranges.ram, glyph = Glyph.MEMORY,
            valueLabel = resources.pssBytes?.let { volume(it) } ?: "—")
        Hint("Весь процесс, включая Rust. CPU: 100% — одно ядро; RAM — оценка памяти процесса.")
    }
}
