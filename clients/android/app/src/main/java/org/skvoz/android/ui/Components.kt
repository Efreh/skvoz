package org.skvoz.android.ui

import androidx.compose.foundation.Canvas
import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.foundation.selection.selectable
import androidx.compose.foundation.selection.selectableGroup
import androidx.compose.material3.*
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.Alignment
import androidx.compose.ui.draw.clip
import androidx.compose.ui.draw.drawWithCache
import androidx.compose.ui.geometry.CornerRadius
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.geometry.Size
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.lerp
import androidx.compose.ui.semantics.*
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.Dp
import org.skvoz.android.*

internal fun Modifier.panelTone() = drawWithCache {
    val brush = Brush.linearGradient(listOf(lerp(SkvozColors.Panel, SkvozColors.Line, 0.2f), SkvozColors.Panel,
        lerp(SkvozColors.Panel, SkvozColors.Background, 0.35f)))
    onDrawBehind { drawRect(brush) }
}

internal fun Modifier.accentTone(enabled: Boolean = true) = if (!enabled) this else drawWithCache {
    val brush = Brush.verticalGradient(listOf(lerp(SkvozColors.Accent, SkvozColors.Light, 0.2f), SkvozColors.Accent))
    onDrawBehind { drawRect(brush) }
}

@Composable internal fun AppHeader(title: String, home: Boolean, loaded: Boolean, menu: Boolean,
    onMenu: (Boolean) -> Unit, up: () -> Unit, settings: () -> Unit, diagnostics: () -> Unit) {
    Column(Modifier.windowInsetsPadding(WindowInsets.safeDrawing.only(WindowInsetsSides.Top + WindowInsetsSides.Horizontal))) {
        Row(Modifier.fillMaxWidth().heightIn(min = SkvozLayout.HeaderHeight).padding(horizontal = 8.dp), verticalAlignment = Alignment.CenterVertically) {
            if (!home) IconButton(up) { Symbol(Glyph.BACK, "Назад") } else Spacer(Modifier.width(8.dp))
            Text(title, Modifier.weight(1f).padding(vertical = 8.dp), style = MaterialTheme.typography.titleLarge,
                maxLines = if (LocalDensity.current.fontScale >= 1.5f) 1 else 2, overflow = TextOverflow.Ellipsis)
            if (home && loaded) Box {
                IconButton({ onMenu(true) }) { Symbol(Glyph.MENU, "Меню") }
                DropdownMenu(menu, { onMenu(false) }) {
                    DropdownMenuItem(text = { Text("Настройки") }, onClick = settings, leadingIcon = { Symbol(Glyph.SETTINGS) })
                    DropdownMenuItem(text = { Text("Диагностика") }, onClick = diagnostics, leadingIcon = { Symbol(Glyph.JOURNAL) })
                }
            }
        }
        HorizontalDivider(color = SkvozColors.Line)
    }
}

@Composable internal fun Symbol(glyph: Glyph, description: String? = null, color: Color = SkvozColors.Secondary) =
    Icon(glyphs.getValue(glyph), description, Modifier.size(SkvozLayout.IconSize), tint = color)

@Composable internal fun Panel(modifier: Modifier = Modifier, padding: Dp = SkvozLayout.PanelPadding,
    spacing: Dp = SkvozLayout.ContentGap, content: @Composable ColumnScope.() -> Unit) {
    Surface(modifier.fillMaxWidth(), shape = MaterialTheme.shapes.medium, color = Color.Transparent, border = BorderStroke(1.dp, SkvozColors.Line)) {
        Column(Modifier.panelTone().padding(padding), verticalArrangement = Arrangement.spacedBy(spacing), content = content)
    }
}

@Composable internal fun FormPage(content: @Composable ColumnScope.() -> Unit) {
    Box(Modifier.fillMaxSize(), contentAlignment = Alignment.TopCenter) {
        Column(Modifier.widthIn(max = 560.dp).fillMaxWidth().verticalScroll(rememberScrollState()).padding(SkvozLayout.PagePadding),
            verticalArrangement = Arrangement.spacedBy(SkvozLayout.SectionGap), content = content)
    }
}

@Composable internal fun Hint(text: String, warning: Boolean = false) {
    Text(text, color = if (warning) SkvozColors.Warning else SkvozColors.Secondary, style = MaterialTheme.typography.bodySmall)
}
@Composable internal fun SectionTitle(text: String) = Text(text, style = MaterialTheme.typography.titleMedium,
    modifier = Modifier.semantics { heading() })

@Composable internal fun Notice(text: String, error: Boolean = false) {
    Surface(Modifier.fillMaxWidth(), color = SkvozColors.Panel, shape = MaterialTheme.shapes.small,
        border = BorderStroke(1.dp, if (error) SkvozColors.Error else SkvozColors.Warning)) {
        Text(text, Modifier.padding(12.dp), color = if (error) SkvozColors.Error else SkvozColors.Warning, style = MaterialTheme.typography.bodyMedium)
    }
}

@Composable internal fun TaskRow(title: String, subtitle: String, glyph: Glyph, onClick: () -> Unit, enabled: Boolean = true) {
    Surface(Modifier.fillMaxWidth(), color = Color.Transparent, shape = MaterialTheme.shapes.medium, border = BorderStroke(1.dp, SkvozColors.Line)) {
        Row(Modifier.panelTone().clickable(enabled = enabled, onClick = onClick).heightIn(min = SkvozLayout.SummaryHeight)
            .padding(horizontal = SkvozLayout.PanelPadding, vertical = SkvozLayout.ContentGap),
            verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(SkvozLayout.ContentGap)) {
            Symbol(glyph, color = SkvozColors.Text)
            Column(Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(2.dp)) {
                Text(title, style = MaterialTheme.typography.titleSmall)
                Hint(subtitle)
            }
            Symbol(Glyph.NEXT)
        }
    }
}

@Composable internal fun PrimaryButton(label: String, onClick: () -> Unit, enabled: Boolean = true) {
    Button(onClick, Modifier.fillMaxWidth().heightIn(min = SkvozLayout.TouchHeight).clip(MaterialTheme.shapes.small).accentTone(enabled),
        enabled = enabled, shape = MaterialTheme.shapes.small,
        colors = ButtonDefaults.buttonColors(containerColor = Color.Transparent),
        contentPadding = PaddingValues(horizontal = 16.dp, vertical = 8.dp)) { Text(label, style = MaterialTheme.typography.labelLarge) }
}
@Composable internal fun SecondaryButton(label: String, onClick: () -> Unit, enabled: Boolean = true, color: Color = SkvozColors.Accent) {
    OutlinedButton(onClick, Modifier.fillMaxWidth().heightIn(min = SkvozLayout.TouchHeight), enabled = enabled, shape = MaterialTheme.shapes.small,
        border = BorderStroke(1.dp, if (enabled) color else SkvozColors.Line),
        colors = ButtonDefaults.outlinedButtonColors(contentColor = color),
        contentPadding = PaddingValues(horizontal = 16.dp, vertical = 8.dp)) { Text(label, style = MaterialTheme.typography.labelLarge) }
}

@Composable internal fun ModeChoice(mode: Mode, onMode: (Mode) -> Unit, enabled: Boolean, alwaysOn: Boolean) {
    Surface(Modifier.fillMaxWidth(), color = SkvozColors.Panel, shape = MaterialTheme.shapes.small,
        border = BorderStroke(1.dp, SkvozColors.Border)) {
        Row(Modifier.fillMaxWidth().selectableGroup()) {
            Mode.entries.forEach { item ->
                val selected = item == mode
                val allowed = enabled && !(alwaysOn && item == Mode.PROXY)
                Surface(Modifier.weight(1f), color = if (selected) SkvozColors.Accent else SkvozColors.Panel,
                    shape = MaterialTheme.shapes.small) {
                    Row((if (selected) Modifier.accentTone(allowed) else Modifier.panelTone())
                        .selectable(selected, enabled = allowed, role = Role.RadioButton, onClick = { onMode(item) })
                        .heightIn(min = SkvozLayout.TouchHeight).padding(horizontal = 12.dp, vertical = 8.dp),
                        horizontalArrangement = Arrangement.Center, verticalAlignment = Alignment.CenterVertically) {
                        Text(if (item == Mode.VPN) "ВПН" else "Прокси", style = MaterialTheme.typography.labelLarge,
                            color = if (!allowed) SkvozColors.Secondary else if (selected) SkvozColors.OnAccent else SkvozColors.Text)
                    }
                }
            }
        }
    }
    if (alwaysOn) Hint("Прокси недоступен, пока Always-on управляет ВПН в Android.")
}

@Composable internal fun Meter(label: String, value: ULong, range: ULong, rate: Boolean = false, glyph: Glyph? = null,
    speedFormat: SpeedFormat = SpeedFormat.BYTES, valueLabel: String? = null, rangeLabel: String? = null) {
    val formatted = valueLabel ?: if (rate) speed(value, speedFormat) else volume(value)
    val scale = rangeLabel ?: if (rate) speed(range, speedFormat) else volume(range)
    val stacked = LocalDensity.current.fontScale >= 1.5f
    Column(Modifier.fillMaxWidth().clearAndSetSemantics { contentDescription = "$label: $formatted. Шкала от нуля до $scale" },
        verticalArrangement = Arrangement.spacedBy(SkvozLayout.DetailGap)) {
        Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            if (glyph != null) Symbol(glyph, color = SkvozColors.Accent)
            Text(label, Modifier.weight(1f), style = MaterialTheme.typography.bodyMedium, color = SkvozColors.Secondary)
            if (!stacked) Text(formatted, style = MaterialTheme.typography.titleSmall, color = SkvozColors.Text)
        }
        if (stacked) Text(formatted, style = MaterialTheme.typography.titleMedium, color = SkvozColors.Text)
        val lit = litSegments(value, range)
        Canvas(Modifier.fillMaxWidth().height(SkvozLayout.MeterHeight)) {
            val gap = 2.dp.toPx().coerceAtMost(size.width / 100)
            val width = ((size.width - gap * 39) / 40).coerceAtLeast(0f)
            val brush = Brush.verticalGradient(listOf(SkvozColors.Light, SkvozColors.Accent.copy(alpha = 0.65f)))
            repeat(40) { index ->
                val origin = Offset(index * (width + gap), 0f)
                if (index < lit) drawRoundRect(brush, origin, Size(width, size.height), CornerRadius(1.dp.toPx()))
                else drawRoundRect(Color(0xFF303A34), origin, Size(width, size.height), CornerRadius(1.dp.toPx()))
            }
        }
        Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
            Text("0", style = MaterialTheme.typography.bodySmall, color = SkvozColors.Secondary)
            Text(scale, style = MaterialTheme.typography.bodySmall, color = SkvozColors.Secondary)
        }
    }
}

@Composable internal fun UriRow(uri: String, onCopy: (String) -> Unit) {
    Row(Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically) {
        Text(uri, Modifier.weight(1f), fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodyMedium)
        IconButton(onClick = { onCopy(uri) }, Modifier.sizeIn(minWidth = 48.dp, minHeight = 48.dp)) { Symbol(Glyph.COPY, "Копировать $uri") }
    }
}
