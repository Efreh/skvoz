package org.skvoz.android

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

internal val wireJson = Json { encodeDefaults = true; explicitNulls = true; ignoreUnknownKeys = false }
internal class ClientFailure(val code: String) : Exception(code)

@Serializable enum class Mode { PROXY, VPN }
@Serializable enum class AppPolicy { EXCLUDE, INCLUDE }
@Serializable data class SealedPassword(val iv: String, val ciphertext: String)
@Serializable data class Settings(
    val schema: Int = 1,
    val endpoint: String = "",
    val login: String = "",
    val password: SealedPassword? = null,
    val device: String = "",
    val mode: Mode = Mode.PROXY,
    val httpPort: Int = 18080,
    val socksPort: Int = 18081,
    val appPolicy: AppPolicy = AppPolicy.EXCLUDE,
    val packages: Set<String> = emptySet(),
    val customCa: Boolean = false,
) {
    fun validate(requireProfile: Boolean = false) {
        if (schema != 1) throw ClientFailure("unsupported_settings")
        if (endpoint.length > 260 || login.length > 64 || packages.size > 512 ||
            packages.any { it.length > 255 || !it.matches(Regex("[A-Za-z0-9_.]+")) } ||
            device.isNotEmpty() && !device.matches(Regex("[a-f0-9]{32}")) ||
            httpPort !in 1..65535 || socksPort !in 1..65535 || httpPort == socksPort ||
            (password?.iv?.length ?: 0) > 32 || (password?.ciphertext?.length ?: 0) > 8192
        ) throw ClientFailure("invalid_settings")
        if (requireProfile) {
            Endpoint.parse(endpoint)
            if (!login.matches(Regex("[A-Za-z0-9_-]{1,64}")) || login == "__skvoz_server" || password == null || device.isEmpty())
                throw ClientFailure("profile_incomplete")
            if (mode == Mode.VPN && appPolicy == AppPolicy.INCLUDE && packages.isEmpty())
                throw ClientFailure("empty_app_selection")
        }
    }
}
internal data class Endpoint(val host: String, val port: Int) {
    companion object {
        fun parse(text: String): Endpoint {
            val match = Regex("^(?:\\[([0-9a-fA-F:.]+)\\]|([A-Za-z0-9._-]+)):(\\d{1,5})$").matchEntire(text)
                ?: throw ClientFailure("invalid_address")
            val host = match.groupValues[1].ifEmpty { match.groupValues[2] }
            val port = match.groupValues[3].toIntOrNull() ?: 0
            if (host.isEmpty() || host.length > 253 || port !in 1..65535) throw ClientFailure("invalid_address")
            return Endpoint(host, port)
        }
    }
}
@Serializable internal data class JournalEntry(val time: Long, val text: String)
internal data class ConnectionState(
    val phase: String = "disconnected", val error: String? = null, val mode: Mode = Mode.PROXY,
    val uploaded: Long = 0, val downloaded: Long = 0, val upRate: Long = 0, val downRate: Long = 0,
    val started: Long = 0, val elapsed: Long = 0, val alwaysOn: Boolean = false,
    val lockdown: Boolean = false, val journal: List<JournalEntry> = emptyList(),
) { val active get() = phase !in setOf("disconnected", "error") }
internal fun journal(previous: List<JournalEntry>, time: Long, code: String): List<JournalEntry> {
    return previous.takeLast(199) + JournalEntry(time, journalCode(code))
}
internal fun retryable(code: String) = code in setOf("server_unavailable", "network_unavailable", "runtime_lost", "timeout", "enrollment_failed", "enrollment_cancelled")

internal fun failureCode(error: Exception): String {
    if (error is kotlinx.coroutines.CancellationException) throw error
    if (error is ClientFailure) return error.code
    val message = error.message ?: return "native_failed"
    return message.takeIf { it.startsWith("Rust error: ") }?.removePrefix("Rust error: ")
        ?.takeIf { it.matches(Regex("[a-z_]{1,64}")) } ?: "native_failed"
}

internal fun ConnectionState.rejectedForeground(time: Long): ConnectionState = copy(
    error = "foreground_start_denied", journal = journal(journal, time, "foreground_start_denied"),
)

internal fun ConnectionState.profileApplyEnabled(loaded: Boolean, busy: Boolean, startupPending: Boolean): Boolean =
    (active || alwaysOn) && loaded && !busy && !startupPending && phase != "stopping"

// Durable diagnostics admit only known identifiers, never arbitrary server/error text.
internal fun journalCode(code: String): String = if (code in JOURNAL_CODES) code else "unknown_event"
private val JOURNAL_CODES = setOf(
    "process_started", "service_started", "service_restarted", "service_destroyed", "vpn_revoked", "user_stop", "connection_cancelled",
    "preparing", "enrolling", "connecting", "connected", "reconnecting", "stopping", "disconnected", "error", "unknown_event",
    "journal_read_failed", "journal_write_failed", "authentication_failed", "certificate_failed", "enrollment_failed", "enrollment_cancelled",
    "server_unavailable", "network_unavailable", "runtime_lost", "timeout", "unsupported_version", "unsupported_family", "version_mismatch",
    "invalid_request", "invalid_state", "unknown_handle", "forbidden", "closed", "random_failed", "invalid_login", "invalid_password", "overloaded", "local_setup_failed", "permission_denied", "foreground_start_denied",
    "foreground_start_failed", "vpn_permission_required", "vpn_establish_failed", "selected_app_missing", "empty_app_selection",
    "invalid_address", "invalid_configuration", "invalid_settings", "unsupported_settings", "profile_incomplete", "mode_mismatch",
    "settings_read_failed", "settings_write_failed", "settings_budget_exceeded", "credential_key_missing", "credential_decrypt_failed",
    "credential_encrypt_failed", "credential_corrupt", "trusted_ca_unavailable", "trusted_ca_export_failed", "ca_budget_exceeded", "invalid_ca",
    "native_failed", "native_internal", "native_panic", "native_start_failed", "native_shutdown_failed", "native_already_active",
    "native_budget_exceeded", "native_overloaded", "native_event_overflow", "native_invalid_response", "native_unexpected_response",
    "native_invalid_event", "native_invalid_counters", "native_request_exhausted", "invalid_native_request", "stale_native_handle",
    "invalid_descriptor", "unexpected_descriptor", "invalid_tun", "invalid_mtu", "invalid_enrollment_token", "enrollment_already_active", "jni_failed",
)
internal fun restoreJournal(stored: List<JournalEntry>, current: List<JournalEntry>) =
    (stored + current).takeLast(200).map { it.copy(text = journalCode(it.text)) }

internal fun apiFailureCode(code: String) = if (code == "closed") "runtime_lost" else code
