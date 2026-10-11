package org.skvoz.android

import kotlinx.coroutines.*
import kotlinx.coroutines.test.*
import org.junit.Assert.*
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@OptIn(ExperimentalCoroutinesApi::class)
class ConnectionPowerTest {
    @Test fun idleConnectionRenewsBeyondTimeoutAndStopCancelsRenewal() = runTest {
        var held = false
        var acquisitions = 0
        var releases = 0
        val connection = launch {
            keepConnectionAwake({ timeout ->
                assertEquals(120_000L, timeout)
                held = true; acquisitions++
            }, { held = false; releases++ }, StandardTestDispatcher(testScheduler)) { awaitCancellation() }
        }
        runCurrent()
        assertTrue(held); assertEquals(1, acquisitions)
        advanceTimeBy(180_001); runCurrent()
        assertTrue(held); assertEquals(4, acquisitions)
        connection.cancelAndJoin()
        assertFalse(held); assertEquals(1, releases)
        advanceTimeBy(180_001); runCurrent()
        assertEquals(4, acquisitions)
    }

    @Test fun replacementWaitsForNativeRetirementBeforeReleasingOldHold() = runTest {
        var held = false
        val retirement = CompletableDeferred<Unit>()
        val retiring = CompletableDeferred<Unit>()
        val connection = launch {
            keepConnectionAwake({ held = true }, { held = false }) {
                try { awaitCancellation() }
                finally {
                    withContext(NonCancellable) {
                        retiring.complete(Unit); retirement.await()
                        assertTrue(held)
                    }
                }
            }
        }
        runCurrent(); connection.cancel(); retiring.await()
        assertTrue(held)
        retirement.complete(Unit); connection.join()
        assertFalse(held)
        keepConnectionAwake({ assertFalse(held); held = true }, { held = false }) { assertTrue(held) }
        assertFalse(held)
    }

    @Test fun terminalFailureReleasesAndCleanupCannotReplaceItsCode() = runTest {
        var released = false
        val failure = ClientFailure("forbidden")
        val cleanup = IllegalStateException("Release failed")
        val caught = try {
            keepConnectionAwake({}, { released = true; throw cleanup }) { throw failure }
            null
        } catch (error: Exception) { error }
        assertTrue(released); assertSame(failure, caught)
        assertSame(cleanup, caught!!.suppressed.single())
        assertFalse(retryable(failureCode(caught)))
    }

    @Test fun deniedAcquisitionDoesNotStartNativeWorkOrRenew() = runTest {
        var acquisitions = 0; var releases = 0; var entered = false
        val failure = ClientFailure("background_execution_unavailable")
        val caught = try {
            keepConnectionAwake({ acquisitions++; throw failure }, { releases++ }) { entered = true }
            null
        } catch (error: Exception) { error }
        assertSame(failure, caught); assertFalse(entered)
        advanceTimeBy(180_001); runCurrent()
        assertEquals(1, acquisitions); assertEquals(1, releases)
        assertEquals(failure.code, journalCode(failure.code))
        assertFalse(retryable(failure.code))
    }

    @Test fun renewalFailureCancelsConnectionAndReleasesTheHold() = runTest {
        var acquisitions = 0; var held = false; var retired = false
        val failure = ClientFailure("background_execution_unavailable")
        val caught = try {
            keepConnectionAwake({
                if (++acquisitions == 2) throw failure
                held = true
            }, { held = false }, StandardTestDispatcher(testScheduler)) {
                try { awaitCancellation() } finally { retired = true }
            }
            null
        } catch (error: Exception) { error }
        assertSame(failure, caught); assertTrue(retired); assertFalse(held)
        assertEquals(2, acquisitions)
    }

    @Test fun renewalContinuesWhileNativeOwnerBlocksWithoutSuspending() = runBlocking {
        Executors.newSingleThreadExecutor().asCoroutineDispatcher().use { nativeWorker ->
            withContext(nativeWorker) {
                val renewed = CountDownLatch(2)
                var released = false
                keepConnectionAwake({ renewed.countDown() }, { released = true }, renewEveryMs = 10) {
                    // JNI polling does not suspend Kotlin; renewal must use another worker.
                    assertTrue("Renewal was starved by blocking native work", renewed.await(2, TimeUnit.SECONDS))
                }
                assertTrue(released)
            }
        }
    }
}
