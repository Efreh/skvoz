package org.skvoz.android

import kotlinx.serialization.json.*

internal val BASIC_METRICS = listOf("packet_in", "packet_out", "packet_dropped", "queue_bytes", "queue_records", "buffer_bytes", "buffer_records")
internal val DETAILED_METRICS = listOf("collection", "samples", "elapsed_ms", "turns", "native_us", "drive_us", "read_full", "write_full", "read_block", "write_block", "read_paused", "core_turns", "core_turn_us", "core_progress", "core_idle_count", "core_idle_us", "core_output_us")
internal data class DiagnosticSnapshot(val enabled: Boolean, val age: ULong?, val values: Map<String, ULong>) {
    val fresh get() = enabled && age != null && age <= 3000uL
}
internal data class DiagnosticState(val basic: Map<String, ULong> = emptyMap(), val detail: DiagnosticSnapshot? = null, val code: String? = null, val run: Long = 0)
internal data class DiagnosticControls(val open: Boolean = false, val detailed: Boolean = false)
// Control epochs preserve rapid UI OFF/ON even while the native IO owner is polling.
internal class DiagnosticControl {
    private val request = java.util.concurrent.atomic.AtomicLong(0)
    private val lock = Any()
    fun current(): Long = request.get()
    fun request(enabled: Boolean, changed: () -> Unit) = synchronized(lock) {
        val old = request.get()
        if (old.and(1L) != (if (enabled) 1L else 0L)) {
            request.set(((old.ushr(1) + 1).shl(1)) or (if (enabled) 1L else 0L))
            changed()
        }
    }
    fun publish(expected: Long, action: () -> Unit) = synchronized(lock) {
        if (request.get() == expected && expected.and(1L) != 0L) action()
    }
}
internal class DiagnosticCollector(
    private val wanted: () -> Long,
    private val read: (Boolean) -> String,
    private val deliver: (Long, DiagnosticSnapshot?, String?) -> Unit,
    private val clock: () -> Long,
) {
    private var request = 0L
    private var at = 0L
    fun collect() {
        val current = wanted()
        val enabled = current.and(1L) != 0L
        if (current == request && !enabled) return
        val now = clock()
        if (current == request && now - at < 1000) return
        val reset = current != request && enabled && request.and(1L) != 0L
        request = current; at = now
        try {
            // The intervening OFF may have been conflated before this IO turn.
            if (reset) read(false)
            deliver(current, diagnosticSnapshot(read(enabled)), null)
        } catch (cancel: kotlinx.coroutines.CancellationException) { throw cancel }
          catch (_: Exception) { deliver(current, null, "diagnostics_unavailable") }
    }
}
private fun JsonElement.numeric(): ULong {
    val primitive = this as? JsonPrimitive ?: throw ClientFailure("diagnostics_unavailable")
    if (primitive.isString || !primitive.content.matches(Regex("[0-9]{1,20}"))) throw ClientFailure("diagnostics_unavailable")
    return primitive.content.toULongOrNull() ?: throw ClientFailure("diagnostics_unavailable")
}
internal fun basicMetrics(counters: JsonObject): Map<String, ULong> = BASIC_METRICS.mapNotNull { key -> counters[key]?.let { key to it.numeric() } }.toMap()
internal fun diagnosticSnapshot(raw: String): DiagnosticSnapshot {
    if (raw.encodeToByteArray().size > 4096) throw ClientFailure("diagnostics_unavailable")
    val json = wireJson.parseToJsonElement(raw).jsonObject
    if (json.keys != (DETAILED_METRICS + listOf("enabled", "sample_age_ms")).toSet()) throw ClientFailure("diagnostics_unavailable")
    val enabled = json.getValue("enabled").jsonPrimitive.let { if (it.isString) null else it.booleanOrNull } ?: throw ClientFailure("diagnostics_unavailable")
    val age = json.getValue("sample_age_ms").let { if (it == JsonNull) null else it.numeric() }
    if (!enabled && age != null) throw ClientFailure("diagnostics_unavailable")
    return DiagnosticSnapshot(enabled, age, DETAILED_METRICS.associateWith { json.getValue(it).numeric() })
}
// Explicit fields only: never stringify profile, raw events, exceptions or device state.
internal fun diagnosticReport(state: ConnectionState, diagnostics: DiagnosticState): String = buildString {
    appendLine("SKVOZ Android ${BuildConfig.VERSION_NAME}; runtime=0.5.0; API=1; network=5; Core=4.1.0")
    appendLine("phase=${journalCode(state.phase)}; error=${state.error?.let(::journalCode) ?: "none"}")
    BASIC_METRICS.forEach { key -> diagnostics.basic[key]?.let { appendLine("$key=$it") } }
    val detail = diagnostics.detail
    appendLine("detailed=${detail?.enabled == true}; fresh=${detail?.fresh == true}; diagnostic_error=${if (diagnostics.code == null) "none" else "diagnostics_unavailable"}")
    if (detail != null) {
        appendLine("sample_age_ms=${detail.age?.toString() ?: "pending"}")
        DETAILED_METRICS.forEach { key -> detail.values[key]?.let { appendLine("$key=$it") } }
    }
    append("Времена — прошедшие микросекунды, не CPU time; счётчики сбрасываются при новом сборе/runtime.")
}
