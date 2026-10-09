package org.skvoz.android.ui

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.*
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.selection.*
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.drawWithCache
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.Path
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.semantics.*
import androidx.compose.ui.text.input.KeyboardCapitalization
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import org.skvoz.android.*

@Composable internal fun ApplicationsScreen(editor: Editor, state: ConnectionState, startup: Boolean,
    select: (String, Boolean) -> Unit, policy: (AppPolicy) -> Unit, refresh: () -> Unit, done: () -> Unit) {
    var search by rememberSaveable { mutableStateOf("") }
    var searchLimit by remember { mutableStateOf(false) }
    val enabled = !editor.busy && !startup && state.phase != "stopping"
    val filtered = remember(editor.apps, search) { filteredApps(editor.apps, search) }
    LaunchedEffect(Unit) { refresh() }
    val fontScale = LocalDensity.current.fontScale
    val icons = rememberAppIconLoader()
    BoxWithConstraints(Modifier.fillMaxSize(), contentAlignment = Alignment.TopCenter) {
        val inlineActions = maxHeight < 360.dp || fontScale >= 1.5f
        Column(Modifier.widthIn(max = 560.dp).fillMaxWidth()) {
            LazyColumn(Modifier.weight(1f), contentPadding = PaddingValues(SkvozLayout.PagePadding)) {
                if (inlineActions) item { Column(Modifier.padding(bottom = SkvozLayout.ContentGap)) { PrimaryButton("Готово", done) } }
                item {
                    Column(Modifier.selectableGroup().padding(bottom = SkvozLayout.ContentGap)) {
                        listOf(AppPolicy.INCLUDE, AppPolicy.EXCLUDE).forEach { item ->
                            val selected = editor.settings.appPolicy == item
                            Row(Modifier.fillMaxWidth().selectable(selected, enabled = enabled, role = Role.RadioButton, onClick = { policy(item) })
                                .heightIn(min = SkvozLayout.TouchHeight).padding(vertical = 4.dp),
                                verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                                RadioButton(selected, onClick = null, enabled = enabled, modifier = Modifier.size(20.dp).clearAndSetSemantics {})
                                Text(if (item == AppPolicy.INCLUDE) "Только выбранные" else "Все, кроме выбранных", Modifier.weight(1f),
                                    style = MaterialTheme.typography.bodyLarge, color = if (selected) SkvozColors.Accent else SkvozColors.Text)
                            }
                        }
                    }
                }
                item {
                    Column(Modifier.padding(bottom = SkvozLayout.ContentGap), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                        Hint("Выбрано ${editor.settings.packages.size} / 512")
                        Hint(if (editor.settings.appPolicy == AppPolicy.INCLUDE) "Через ВПН идут только выбранные приложения." else "Через ВПН идут все приложения, кроме выбранных и SKVOZ.")
                        if (editor.settings.appPolicy == AppPolicy.INCLUDE && editor.settings.packages.isEmpty()) Notice("Выберите хотя бы одно приложение.", true)
                        if (state.lockdown) Notice("Блокировка вне ВПН включена. Исключённые приложения блокируются Android.")
                        editor.fieldErrors[ProfileField.APPLICATIONS]?.let { Notice(errorText(it), true) }
                        OutlinedTextField(search, { value ->
                            searchLimit = value.length > 256
                            if (!searchLimit) search = value
                        }, Modifier.fillMaxWidth(), label = { Text("Поиск по имени или package") }, singleLine = true,
                            isError = searchLimit, supportingText = if (searchLimit) ({ Text("Поиск: не более 256 символов.") }) else null,
                            keyboardOptions = KeyboardOptions(capitalization = KeyboardCapitalization.None, autoCorrectEnabled = false), shape = MaterialTheme.shapes.small)
                    }
                }
                if (editor.appsLoading) item { Row(Modifier.padding(bottom = SkvozLayout.ContentGap), verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                    CircularProgressIndicator(Modifier.size(24.dp), strokeWidth = 2.dp); Hint("Загрузка приложений…")
                } }
                editor.appsError?.let { code -> item {
                    Column(Modifier.padding(bottom = SkvozLayout.ContentGap), verticalArrangement = Arrangement.spacedBy(SkvozLayout.ContentGap)) {
                    Notice(errorText(code), true)
                    SecondaryButton("Обновить список", refresh, !editor.appsLoading)
                    }
                } }
                if (!editor.appsLoading && editor.appsError == null && filtered.isEmpty()) item {
                    Column(Modifier.padding(bottom = SkvozLayout.ContentGap), verticalArrangement = Arrangement.spacedBy(SkvozLayout.ContentGap)) {
                    Hint(if (search.isEmpty()) "Нет доступных приложений." else "Ничего не найдено. Выбор сохранён.")
                    if (search.isNotEmpty()) SecondaryButton("Очистить поиск", { search = ""; searchLimit = false })
                    }
                }
                itemsIndexed(filtered, key = { _, app -> app.name }) { index, app ->
                    val checked = app.name in editor.settings.packages
                    val first = index == 0
                    val last = index == filtered.lastIndex
                    Surface(color = Color.Transparent, shape = RoundedCornerShape(
                        topStart = if (first) 6.dp else 0.dp, topEnd = if (first) 6.dp else 0.dp,
                        bottomStart = if (last) 6.dp else 0.dp, bottomEnd = if (last) 6.dp else 0.dp)) {
                        Row(Modifier.fillMaxWidth().panelTone().appRowFrame(first, last)
                            .toggleable(checked, enabled = enabled && (!app.missing || checked), role = Role.Checkbox,
                            onValueChange = { select(app.name, it) }).heightIn(min = SkvozLayout.ApplicationHeight)
                            .padding(horizontal = 12.dp, vertical = 8.dp), verticalAlignment = Alignment.CenterVertically,
                            horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                            AppIcon(app.name, app.missing, icons)
                            Column(Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(2.dp)) {
                                Text(app.label, style = MaterialTheme.typography.bodyLarge,
                                    maxLines = if (fontScale >= 1.5f) Int.MAX_VALUE else 1, overflow = TextOverflow.Ellipsis)
                                Text(if (app.missing) "Удалено: снимите выбор" else app.name,
                                    style = MaterialTheme.typography.bodySmall, color = if (app.missing) SkvozColors.Warning else SkvozColors.Secondary,
                                    maxLines = if (fontScale >= 1.5f) Int.MAX_VALUE else 1, overflow = TextOverflow.Ellipsis)
                            }
                            Checkbox(checked, onCheckedChange = null, enabled = enabled, modifier = Modifier.size(20.dp).clearAndSetSemantics {})
                        }
                    }
                }
                item { Column(Modifier.padding(top = SkvozLayout.ContentGap, bottom = SkvozLayout.ContentGap)) {
                    Hint("SKVOZ всегда исключён. Список хранится на телефоне и не управляет трафиком Прокси. Примените изменения кнопкой «${applyLabel(state)}».")
                } }
                item { EditorError(editor) }
            }
            if (!inlineActions) Column(Modifier.padding(horizontal = SkvozLayout.PagePadding, vertical = SkvozLayout.ContentGap), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                Hint("Примените изменения кнопкой «${applyLabel(state)}».")
                PrimaryButton("Готово", done)
            }
        }
    }
}

// Draw one shared edge per virtualized row; only the group ends have rounded corners.
private fun Modifier.appRowFrame(first: Boolean, last: Boolean) = drawWithCache {
    val stroke = 1.dp.toPx()
    val edge = stroke / 2
    val right = size.width - edge
    val bottom = size.height - edge
    val topRadius = if (first) 6.dp.toPx() else edge
    val bottomRadius = if (last) 6.dp.toPx() else edge
    val frame = Path().apply {
        moveTo(edge, if (first) topRadius else 0f)
        if (first) {
            quadraticTo(edge, edge, topRadius, edge)
            lineTo(size.width - topRadius, edge)
            quadraticTo(right, edge, right, topRadius)
        } else { moveTo(right, 0f) }
        lineTo(right, size.height - bottomRadius)
        quadraticTo(right, bottom, size.width - bottomRadius, bottom)
        lineTo(bottomRadius, bottom)
        quadraticTo(edge, bottom, edge, size.height - bottomRadius)
        lineTo(edge, if (first) topRadius else 0f)
    }
    onDrawWithContent { drawContent(); drawPath(frame, SkvozColors.Line, style = Stroke(stroke)) }
}
