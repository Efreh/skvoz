package org.skvoz.android

internal data class NotificationContent(val title: String, val status: String, val down: Long, val up: Long, val format: SpeedFormat = SpeedFormat.BYTES) {
    val expanded get() = "$status\n↓ ${speed(down, format)}   ↑ ${speed(up, format)}"
}
internal fun notificationContent(state: ConnectionState, format: SpeedFormat = SpeedFormat.BYTES) = NotificationContent(
    title = state.display?.endpoint ?: "Соединение SKVOZ", status = phaseText(state.phase),
    down = if (state.phase == "connected") state.downRate.coerceAtLeast(0) else 0,
    up = if (state.phase == "connected") state.upRate.coerceAtLeast(0) else 0,
    format = format,
)
// There is no rate timer or new network poll. State transitions remain visible with the screen off.
internal class NotificationCadence {
    private var previous: NotificationContent? = null
    private var lastRatePost = Long.MIN_VALUE
    fun shouldPublish(next: NotificationContent, now: Long, interactive: Boolean, allowed: Boolean): Boolean {
        val structural = previous?.title != next.title || previous?.status != next.status
        if (structural) { previous = next; lastRatePost = now; return true }
        if (!interactive || !allowed || next == previous) return false
        if (next.format == previous?.format && now - lastRatePost < 1000) return false
        previous = next; lastRatePost = now
        return true
    }
}
