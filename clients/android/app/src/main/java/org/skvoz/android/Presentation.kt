package org.skvoz.android

import kotlin.math.ceil

internal enum class ProfileField { ENDPOINT, LOGIN, PASSWORD, HTTP_PORT, SOCKS_PORT, APPLICATIONS }
internal data class DraftValidation(val settings: Settings?, val errors: Map<ProfileField, String>)

// Raw editor text is validated before constructing the persisted configuration.
internal fun validateDraft(editor: Editor): DraftValidation {
    val errors = linkedMapOf<ProfileField, String>().apply { putAll(editor.rejectedInput) }
    if (editor.settings.endpoint.length > 260 || runCatching { Endpoint.parse(editor.settings.endpoint) }.isFailure)
        errors[ProfileField.ENDPOINT] = "invalid_address"
    if (!editor.settings.login.matches(Regex("[A-Za-z0-9_-]{1,64}")) || editor.settings.login == "__skvoz_server")
        errors[ProfileField.LOGIN] = "invalid_login"
    if (editor.password.encodeToByteArray().size !in 12..72) errors[ProfileField.PASSWORD] = "invalid_password"
    fun port(text: String, field: ProfileField): Int? {
        val value = text.takeIf { it.matches(Regex("[0-9]{1,5}")) }?.toIntOrNull()?.takeIf { it in 1..65535 }
        if (value == null) errors[field] = "invalid_port"
        return value
    }
    val http = port(editor.httpPort, ProfileField.HTTP_PORT)
    val socks = port(editor.socksPort, ProfileField.SOCKS_PORT)
    if (http != null && http == socks) errors[ProfileField.SOCKS_PORT] = "duplicate_ports"
    if (editor.settings.packages.size > 512) errors[ProfileField.APPLICATIONS] = "app_selection_budget_exceeded"
    if (editor.settings.mode == Mode.VPN && editor.settings.appPolicy == AppPolicy.INCLUDE && editor.settings.packages.isEmpty())
        errors[ProfileField.APPLICATIONS] = "empty_app_selection"
    if (editor.settings.mode == Mode.VPN && editor.apps.any { it.missing && it.name in editor.settings.packages })
        errors[ProfileField.APPLICATIONS] = "selected_app_missing"
    return DraftValidation(if (errors.isEmpty()) editor.settings.copy(httpPort = http!!, socksPort = socks!!) else null, errors)
}

internal fun editText(editor: Editor, field: ProfileField, value: String): Editor {
    val limit = when (field) { ProfileField.ENDPOINT -> 260; ProfileField.LOGIN -> 64; ProfileField.PASSWORD -> 1024; else -> 32 }
    if (value.length > limit) return editor.copy(rejectedInput = editor.rejectedInput + (field to "input_too_long"))
    val updated = when (field) {
        ProfileField.ENDPOINT -> editor.copy(settings = editor.settings.copy(endpoint = value))
        ProfileField.LOGIN -> editor.copy(settings = editor.settings.copy(login = value))
        ProfileField.PASSWORD -> editor.copy(password = value)
        ProfileField.HTTP_PORT -> editor.copy(httpPort = value)
        ProfileField.SOCKS_PORT -> editor.copy(socksPort = value)
        ProfileField.APPLICATIONS -> editor
    }
    return updated.copy(rejectedInput = updated.rejectedInput - field)
}

internal data class ConnectionDisplaySnapshot(
    val endpoint: String, val login: String, val mode: Mode, val appPolicy: AppPolicy, val packageCount: Int,
    val httpUri: String? = null, val socksUri: String? = null,
)
internal fun Settings.displaySnapshot() = ConnectionDisplaySnapshot(endpoint, login, mode, appPolicy, packages.size)

internal enum class ConnectionAction { CONNECT, CANCEL, DISCONNECT, VPN_SETTINGS, WAIT }
internal fun connectionAction(state: ConnectionState, startup: Boolean, busy: Boolean): ConnectionAction = when {
    busy || state.phase == "stopping" -> ConnectionAction.WAIT
    state.alwaysOn -> ConnectionAction.VPN_SETTINGS
    startup || state.phase in setOf("preparing", "enrolling", "connecting") -> ConnectionAction.CANCEL
    state.phase in setOf("connected", "reconnecting") -> ConnectionAction.DISCONNECT
    else -> ConnectionAction.CONNECT
}
internal fun applyLabel(state: ConnectionState) = when {
    state.alwaysOn -> "Сохранить и применить"
    state.active -> "Сохранить и переподключить"
    else -> "Подключить"
}

// Display ranges describe scale, never network capacity or a throughput limit.
internal fun expandedRange(current: ULong, value: ULong, strictlyAbove: Boolean = false): ULong {
    var range = current.coerceAtLeast(1uL)
    while (value > range || strictlyAbove && value == range) {
        if (range > ULong.MAX_VALUE / 2uL) return ULong.MAX_VALUE
        range *= 2uL
    }
    return range
}
internal fun litSegments(value: ULong, range: ULong): Int =
    if (value == 0uL) 0 else ceil(value.toDouble() / range.coerceAtLeast(1uL).toDouble() * 40).toInt().coerceIn(1, 40)
internal const val INITIAL_RATE_RANGE = 8388608uL
internal data class DisplayRanges(
    val run: Long = 0, val rate: ULong = INITIAL_RATE_RANGE,
    val queue: ULong = 1024uL, val buffer: ULong = 1024uL,
    val queueInitialized: Boolean = false, val bufferInitialized: Boolean = false,
) {
    fun update(state: ConnectionState, diagnostics: DiagnosticState): DisplayRanges {
        val previous = if (run != state.started) DisplayRanges(run = state.started) else this
        val queueValue = diagnostics.basic["queue_bytes"].takeIf { diagnostics.run == state.started }
        val bufferValue = diagnostics.basic["buffer_bytes"].takeIf { diagnostics.run == state.started }
        return previous.copy(
            rate = expandedRange(previous.rate, maxOf(state.upRate, state.downRate, 0).toULong()),
            queue = if (queueValue == null) previous.queue else expandedRange(previous.queue, queueValue, !previous.queueInitialized),
            buffer = if (bufferValue == null) previous.buffer else expandedRange(previous.buffer, bufferValue, !previous.bufferInitialized),
            queueInitialized = previous.queueInitialized || queueValue != null,
            bufferInitialized = previous.bufferInitialized || bufferValue != null,
        )
    }
}

internal fun volume(bytes: Long): String = volume(bytes.coerceAtLeast(0).toULong())
internal fun volume(bytes: ULong): String = when {
    bytes >= 1073741824uL -> "%.1f ГиБ".format(bytes.toDouble() / 1073741824.0)
    bytes >= 1048576uL -> "%.1f МиБ".format(bytes.toDouble() / 1048576.0)
    bytes >= 1024uL -> "%.1f КиБ".format(bytes.toDouble() / 1024.0)
    else -> "$bytes Б"
}
internal fun duration(milliseconds: Long): String {
    val seconds = milliseconds.coerceAtLeast(0) / 1000
    return "%02d:%02d:%02d".format(seconds / 3600, seconds / 60 % 60, seconds % 60)
}
internal fun filteredApps(apps: List<SelectableApp>, search: String) =
    apps.filter { it.name.contains(search, true) || it.label.contains(search, true) }
