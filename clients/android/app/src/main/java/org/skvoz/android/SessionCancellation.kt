package org.skvoz.android

import kotlinx.coroutines.suspendCancellableCoroutine
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/** Runs on an IO worker; completion waits for the blocking native operation to unwind. */
internal suspend fun <T> cancellableNative(token: Long, cancel: (Long) -> Unit, operation: () -> T): T =
    suspendCancellableCoroutine { continuation ->
        continuation.invokeOnCancellation { cancel(token) }
        try { if (continuation.isActive) continuation.resume(operation()) }
        catch (error: Exception) { continuation.resumeWithException(error) }
    }
/** Every run gets separate signals. Retired callbacks cannot address a later run. */
internal class RunSignals(private val cancel: (Long) -> Unit) {
    val network = AtomicLong(0)
    val enrollment = AtomicLong(0)
    private val active = AtomicBoolean(true)
    fun changed() {
        if (!active.get()) return
        network.incrementAndGet()
        enrollment.get().takeIf { it != 0L }?.let(cancel)
    }
    fun retire() {
        active.set(false)
        enrollment.getAndSet(0).takeIf { it != 0L }?.let(cancel)
    }
}

/** Retirement cannot replace the connection failure or cancellation that caused it. */
internal suspend fun <T> preservingCleanup(cleanup: suspend () -> Unit, operation: suspend () -> T): T {
    var primary: Throwable? = null
    try { return operation() }
    catch (error: Throwable) { primary = error; throw error }
    finally {
        try { cleanup() }
        catch (error: Throwable) { if (primary == null) throw error else if (primary !== error) primary.addSuppressed(error) }
    }
}
