package org.skvoz.android.ui

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.selection.toggleable
import androidx.compose.material3.*
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.platform.LocalLocale
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.dp
import org.skvoz.android.*
import java.text.SimpleDateFormat
import java.util.Date

@Composable internal fun DiagnosticsScreen(state: ConnectionState, diagnostics: DiagnosticState, controls: DiagnosticControls,
    ranges: DisplayRanges, detailed: (Boolean) -> Unit, copy: (String) -> Unit) {
    FormPage {
        JournalPanel(state.journal)
        Panel {
            SectionTitle("Показатели сети")
            if (diagnostics.basic.isEmpty()) Hint("Нет текущего образца.")
            BASIC_METRICS.forEach { key -> diagnostics.basic[key]?.let { value ->
                if (key == "queue_bytes" || key == "buffer_bytes") Meter(metricLabel(key), value, if (key == "queue_bytes") ranges.queue else ranges.buffer)
                else StatLine(metricLabel(key), value.toString())
            } }
            Hint("Резерв памяти относится к сетевому runtime и не показывает RAM приложения. Шкалы показывают абсолютные байты, а не долю заполнения.")
        }
        Panel {
            SectionTitle("Подробные измерения")
            Row(Modifier.fillMaxWidth().toggleable(controls.detailed, role = Role.Switch, onValueChange = detailed).heightIn(min = SkvozLayout.TouchHeight),
                verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                Text("Сбор измерений", Modifier.weight(1f))
                Switch(controls.detailed, onCheckedChange = null, modifier = Modifier.clearAndSetSemantics {})
            }
            val sample = diagnostics.detail
            Hint(when {
                !controls.detailed -> "Подробный сбор выключен"
                diagnostics.code != null -> "Измерения недоступны; соединение продолжает работать."
                sample == null || sample.age == null -> "Ожидание образца"
                !sample.fresh -> "Устаревший образец (${sample.age} мс)"
                else -> "Возраст образца: ${sample.age} мс"
            })
            if (controls.detailed && sample?.enabled == true) DETAILED_METRICS.forEach { key -> sample.values[key]?.let { StatLine(metricLabel(key), it.toString()) } }
            Hint("Времена включают обработку и ожидание, не являются загрузкой CPU. Счётчики сбрасываются при новом сборе/runtime. Выход с экрана или в фон выключает подробный сбор.")
        }
        SecondaryButton("Копировать отчёт", { copy(diagnosticReport(state, diagnostics)) })
        Hint("Отчёт содержит только состояния и разрешённые числовые поля, без адреса и учётных данных.")
    }
}

@Composable internal fun JournalPanel(entries: List<JournalEntry>) {
    val formatter = SimpleDateFormat("dd.MM HH:mm:ss", LocalLocale.current.platformLocale)
    val stacked = LocalDensity.current.fontScale >= 1.5f
    Panel(spacing = SkvozLayout.DetailGap) {
        SectionTitle("Журнал соединения")
        Hint("Последние 20 событий · новые сверху")
        if (entries.isEmpty()) Hint("Событий пока нет")
        entries.takeLast(20).reversed().forEach { entry ->
            if (stacked) Column(Modifier.fillMaxWidth(), verticalArrangement = Arrangement.spacedBy(4.dp)) {
                Text(formatter.format(Date(entry.time)), style = MaterialTheme.typography.bodySmall, color = SkvozColors.Secondary)
                Text(journalCode(entry.text), style = MaterialTheme.typography.bodySmall, fontFamily = FontFamily.Monospace)
            } else Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                Text(formatter.format(Date(entry.time)), style = MaterialTheme.typography.bodySmall, color = SkvozColors.Secondary)
                Text(journalCode(entry.text), Modifier.weight(1f), style = MaterialTheme.typography.bodySmall, fontFamily = FontFamily.Monospace)
            }
            HorizontalDivider(color = SkvozColors.Line)
        }
    }
}
