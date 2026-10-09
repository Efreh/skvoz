package org.skvoz.android.ui

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.relocation.BringIntoViewRequester
import androidx.compose.foundation.relocation.bringIntoViewRequester
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.focus.*
import androidx.compose.ui.platform.LocalFocusManager
import androidx.compose.ui.text.input.*
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.launch
import org.skvoz.android.*

@Composable internal fun ProfileScreen(
    editor: Editor, state: ConnectionState, startup: Boolean, text: (ProfileField, String) -> Unit,
    mode: (Mode) -> Unit, submit: () -> Unit, applications: () -> Unit,
) {
    val enabled = !editor.busy && !startup && state.phase != "stopping"
    val fields = remember { listOf(ProfileField.ENDPOINT, ProfileField.LOGIN, ProfileField.PASSWORD).associateWith { FocusRequester() } }
    FocusErrors(editor, fields)
    FormPage {
        Hint("Один профиль подключения. Изменения применяются при подключении.")
        if (editor.dirty) Notice("Изменения не применены")
        SectionTitle("Режим подключения")
        ModeChoice(editor.settings.mode, mode, enabled, state.alwaysOn)
        Hint(if (editor.settings.mode == Mode.VPN) "ВПН передаёт IP-трафик выбранных приложений." else "Прокси обслуживает приложения с настроенным локальным HTTP или SOCKS5 адресом.")
        EditorField(editor, ProfileField.ENDPOINT, editor.settings.endpoint, "Сервер и порт",
            "server.example:4222 · IPv4:port · [IPv6]:port", text, enabled, fields.getValue(ProfileField.ENDPOINT), KeyboardType.Uri)
        EditorField(editor, ProfileField.LOGIN, editor.settings.login, "Логин", "1–64: латинские буквы, цифры, _ или -",
            text, enabled, fields.getValue(ProfileField.LOGIN), KeyboardType.Ascii)
        EditorField(editor, ProfileField.PASSWORD, editor.password, "Пароль", "12–72 байта UTF-8. Сохраняется на устройстве.",
            text, enabled, fields.getValue(ProfileField.PASSWORD), KeyboardType.Password, true, submit)
        if (editor.settings.mode == Mode.VPN) {
            TaskRow("Приложения ВПН", policySummary(editor.settings.appPolicy, editor.settings.packages.size), Glyph.APPS, applications)
            editor.fieldErrors[ProfileField.APPLICATIONS]?.let { Notice(errorText(it), true) }
        }
        EditorError(editor)
        PrimaryButton(if (editor.busy) "Сохранение…" else applyLabel(state), submit, enabled)
        Hint("Назад оставляет черновик в открытом приложении. Подключение сохраняет профиль и применяет его; старые потоки при переподключении завершаются.")
    }
}

@Composable internal fun ProxyScreen(editor: Editor, state: ConnectionState, startup: Boolean,
    text: (ProfileField, String) -> Unit, submit: () -> Unit, copy: (String) -> Unit) {
    val enabled = !editor.busy && !startup && state.phase != "stopping"
    val fields = remember { listOf(ProfileField.HTTP_PORT, ProfileField.SOCKS_PORT).associateWith { FocusRequester() } }
    FocusErrors(editor, fields)
    FormPage {
        Hint("Порты Прокси на этом устройстве. HTTP/CONNECT и SOCKS5 TCP используют разные порты от 1 до 65535.")
        if (editor.dirty) Notice("Изменения не применены")
        val active = state.display?.takeIf { state.phase == "connected" && state.mode == Mode.PROXY }
        val nextHttp = "http://127.0.0.1:${editor.httpPort.ifEmpty { "порт не указан" }}"
        val nextSocks = "socks5://127.0.0.1:${editor.socksPort.ifEmpty { "порт не указан" }}"
        Panel {
            SectionTitle("HTTP / CONNECT")
            EditorField(editor, ProfileField.HTTP_PORT, editor.httpPort, "Порт HTTP / CONNECT", "По умолчанию 18080", text, enabled, fields.getValue(ProfileField.HTTP_PORT), KeyboardType.Number)
            if (active?.httpUri != nextHttp) { Hint("Следующий адрес · черновик"); Hint(nextHttp) }
            active?.httpUri?.let { Hint("Действующий адрес"); UriRow(it, copy) }
        }
        Panel {
            SectionTitle("SOCKS5 TCP")
            EditorField(editor, ProfileField.SOCKS_PORT, editor.socksPort, "Порт SOCKS5 TCP", "По умолчанию 18081", text, enabled, fields.getValue(ProfileField.SOCKS_PORT), KeyboardType.Number, true, submit)
            if (active?.socksUri != nextSocks) { Hint("Следующий адрес · черновик"); Hint(nextSocks) }
            active?.socksUri?.let { Hint("Действующий адрес"); UriRow(it, copy) }
        }
        EditorError(editor)
        PrimaryButton(if (editor.busy) "Сохранение…" else applyLabel(state), submit, enabled)
        Hint("Порты относятся только к Прокси. Назад оставляет черновик; применение использует общий профиль и выбранный режим.")
    }
}

@Composable private fun FocusErrors(editor: Editor, fields: Map<ProfileField, FocusRequester>) {
    LaunchedEffect(editor.fieldErrors, editor.error) {
        if (editor.error == "profile_validation_failed") fields[editor.fieldErrors.keys.firstOrNull()]?.requestFocus()
    }
}

@Composable private fun EditorField(
    editor: Editor, field: ProfileField, value: String, label: String, hint: String,
    onText: (ProfileField, String) -> Unit, enabled: Boolean, requester: FocusRequester, type: KeyboardType,
    done: Boolean = false, submit: () -> Unit = {},
) {
    val focus = LocalFocusManager.current
    val intoView = remember { BringIntoViewRequester() }
    val scope = rememberCoroutineScope()
    val error = editor.fieldErrors[field]
    OutlinedTextField(value, { onText(field, it) }, modifier = Modifier.fillMaxWidth().heightIn(min = 56.dp)
        .focusRequester(requester).bringIntoViewRequester(intoView).onFocusChanged { if (it.isFocused) scope.launch { intoView.bringIntoView() } },
        enabled = enabled, label = { Text(label) }, singleLine = true, isError = error != null,
        supportingText = { Text(error?.let(::errorText) ?: hint) }, shape = MaterialTheme.shapes.small,
        visualTransformation = if (field == ProfileField.PASSWORD) PasswordVisualTransformation() else VisualTransformation.None,
        keyboardOptions = KeyboardOptions(keyboardType = type, autoCorrectEnabled = false,
            capitalization = KeyboardCapitalization.None, imeAction = if (done) ImeAction.Done else ImeAction.Next),
        keyboardActions = KeyboardActions(onNext = { focus.moveFocus(FocusDirection.Down) }, onDone = { if (enabled) submit() }))
}

@Composable internal fun EditorError(editor: Editor) {
    editor.error?.let { code -> if (code != "profile_validation_failed") Notice(errorText(code), error = code != "notifications_denied") }
}
