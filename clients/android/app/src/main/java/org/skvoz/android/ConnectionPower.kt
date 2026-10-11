package org.skvoz.android

import android.content.Context
import android.os.PowerManager
import kotlinx.coroutines.*

internal const val CONNECTION_WAKE_TIMEOUT_MS = 120_000L
internal const val CONNECTION_WAKE_RENEW_MS = 60_000L

/** One active attempt owns the hold, including native retirement, never the Activity. */
internal suspend fun <T> withConnectionPower(context: Context, operation: suspend () -> T): T {
    val wake = context.getSystemService(PowerManager::class.java)
        .newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "SKVOZ:connection")
        .apply { setReferenceCounted(false) }
    return keepConnectionAwake(
        acquire = { timeout ->
            try { wake.acquire(timeout) }
            catch (_: SecurityException) { throw ClientFailure("background_execution_unavailable") }
        },
        release = { if (wake.isHeld) wake.release() },
        operation = operation,
    )
}

internal suspend fun <T> keepConnectionAwake(
    acquire: (Long) -> Unit,
    release: () -> Unit,
    renewalDispatcher: CoroutineDispatcher = Dispatchers.Default,
    renewEveryMs: Long = CONNECTION_WAKE_RENEW_MS,
    operation: suspend () -> T,
): T = coroutineScope {
    preservingCleanup(cleanup = { release() }) {
        currentCoroutineContext().ensureActive()
        acquire(CONNECTION_WAKE_TIMEOUT_MS)
        // Each OS hold has a timeout in case renewal stops.
        // Native polling blocks the IO worker and may never yield to its local queue.
        val renewal = launch(renewalDispatcher, start = CoroutineStart.UNDISPATCHED) {
            while (isActive) {
                delay(renewEveryMs)
                acquire(CONNECTION_WAKE_TIMEOUT_MS)
            }
        }
        try { operation() }
        finally { withContext(NonCancellable) { renewal.cancelAndJoin() } }
    }
}
