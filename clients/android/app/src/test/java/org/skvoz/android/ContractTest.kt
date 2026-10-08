package org.skvoz.android

import kotlinx.coroutines.*
import org.junit.Assert.*
import org.junit.Test
import java.io.ByteArrayInputStream
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class ContractTest {
    @Test fun rapidDetailedToggleResetsNativeAndRejectsOldCallbacksWithoutDefaultClocks() {
        val control = DiagnosticControl()
        var shown: DiagnosticSnapshot? = null
        var error: String? = null
        var clocks = 0; var time = 1000L
        val calls = mutableListOf<Boolean>()
        fun raw(enabled: Boolean) = kotlinx.serialization.json.buildJsonObject {
            put("enabled", kotlinx.serialization.json.JsonPrimitive(enabled)); put("sample_age_ms", kotlinx.serialization.json.JsonPrimitive(0).takeIf { enabled } ?: kotlinx.serialization.json.JsonNull)
            DETAILED_METRICS.forEach { put(it, kotlinx.serialization.json.JsonPrimitive(1)) }
        }.toString()
        val deliver: (Long, DiagnosticSnapshot?, String?) -> Unit = { request, sample, code -> control.publish(request) { shown = sample; error = code } }
        var fail = false; var cancel = false
        val collector = DiagnosticCollector(control::current, { enabled ->
            calls.add(enabled)
            if (cancel) throw CancellationException("stop")
            if (fail) throw RuntimeException("private exception")
            raw(enabled)
        }, deliver, { clocks++; time })
        repeat(100) { collector.collect() }
        assertEquals(0, clocks); assertTrue(calls.isEmpty())
        control.request(true) { shown = null }; val old = control.current()
        collector.collect(); assertEquals(listOf(true), calls); assertNotNull(shown)
        repeat(100) { collector.collect() }; assertEquals(listOf(true), calls)
        control.request(false) { shown = null }; control.request(true) { shown = null }
        deliver(old, diagnosticSnapshot(raw(true)), null)
        assertNull(shown)
        collector.collect(); assertEquals(listOf(true, false, true), calls); assertNotNull(shown)
        time += 1000; fail = true; collector.collect()
        assertNull(shown); assertEquals("diagnostics_unavailable", error)
        time += 1000; fail = false; cancel = true
        assertThrows(CancellationException::class.java) { collector.collect() }
        control.request(false) { shown = null }; cancel = false; collector.collect()
        assertFalse(calls.last()); assertNull(shown)
        val previousClocks = clocks; repeat(100) { collector.collect() }; assertEquals(previousClocks, clocks)
    }

    @Test fun diagnosticNumbersAreBoundedAndCopyCannotLeakUnlistedData() {
        val json = kotlinx.serialization.json.buildJsonObject {
            put("enabled", kotlinx.serialization.json.JsonPrimitive(true))
            put("sample_age_ms", kotlinx.serialization.json.JsonPrimitive(1000))
            DETAILED_METRICS.forEach { put(it, kotlinx.serialization.json.JsonPrimitive(ULong.MAX_VALUE.toString())) }
        }
        // String numbers and unknown fields are rejected before display or copying.
        assertThrows(ClientFailure::class.java) { diagnosticSnapshot(json.toString()) }
        val numeric = json.toString().replace("\"18446744073709551615\"", "18446744073709551615")
        val snapshot = diagnosticSnapshot(numeric)
        assertTrue(snapshot.fresh); assertEquals(ULong.MAX_VALUE, snapshot.values["native_us"])
        assertFalse(snapshot.copy(age = 3001uL).fresh); assertFalse(snapshot.copy(age = null).fresh)
        assertThrows(ClientFailure::class.java) { diagnosticSnapshot(numeric.dropLast(1) + ",\"endpoint\":\"privatehost\"}") }
        assertThrows(ClientFailure::class.java) { diagnosticSnapshot("x".repeat(4097)) }
        val basic = basicMetrics(kotlinx.serialization.json.buildJsonObject {
            put("packet_in", kotlinx.serialization.json.JsonPrimitive(12)); put("endpoint", kotlinx.serialization.json.JsonPrimitive("privatehost"))
        })
        val report = diagnosticReport(ConnectionState(error = "privatepassword"), DiagnosticState(basic, snapshot))
        assertTrue(report.contains("packet_in=12")); assertTrue(report.contains("native_us=18446744073709551615"))
        assertFalse(report.contains("privatepassword")); assertFalse(report.contains("privatehost")); assertFalse(report.contains("endpoint"))
        assertFalse(diagnosticReport(ConnectionState(), DiagnosticState()).contains("native_us="))
    }

    @Test fun failedAlwaysOnProfileCanBeCorrectedWithoutManualDisconnect() {
        val failed = ConnectionState(phase = "error", mode = Mode.VPN, alwaysOn = true, error = "authentication_failed")
        assertFalse(failed.active)
        assertTrue(failed.profileApplyEnabled(loaded = true, busy = false, startupPending = false))
        assertFalse(failed.profileApplyEnabled(loaded = false, busy = false, startupPending = false))
        assertFalse(failed.profileApplyEnabled(loaded = true, busy = true, startupPending = false))
        assertFalse(failed.profileApplyEnabled(loaded = true, busy = false, startupPending = true))
        assertFalse(failed.copy(phase = "stopping").profileApplyEnabled(true, false, false))
        assertFalse(failed.copy(alwaysOn = false).profileApplyEnabled(true, false, false))
    }

    @Test fun rejectedModeReplacementKeepsTheCurrentSessionVisible() {
        val current = ConnectionState(phase = "connected", mode = Mode.PROXY, uploaded = 123, downRate = 456)
        val rejected = current.rejectedForeground(789)
        assertTrue(rejected.active); assertEquals(current.phase, rejected.phase)
        assertEquals(current.mode, rejected.mode); assertEquals(123L, rejected.uploaded)
        assertEquals(456L, rejected.downRate)
        assertEquals("foreground_start_denied", rejected.error)
        assertEquals(JournalEntry(789, "foreground_start_denied"), rejected.journal.last())
    }

    @Test fun nativeErrorsKeepExactCodesWithoutAdmittingRawMessagesOrCancellation() {
        assertEquals("authentication_failed", failureCode(RuntimeException("Rust error: authentication_failed")))
        assertEquals("server_unavailable", failureCode(RuntimeException("Rust error: server_unavailable")))
        assertTrue(retryable(failureCode(RuntimeException("Rust error: server_unavailable"))))
        assertTrue(retryable(apiFailureCode("closed")))
        assertEquals("overloaded", apiFailureCode("overloaded"))
        assertFalse(retryable(apiFailureCode("overloaded")))
        assertFalse(retryable(failureCode(RuntimeException("Rust error: authentication_failed"))))
        listOf("privatepassword", "Some error: server_unavailable", "Rust error: secret=value", "Rust error: authentication_failed\nprivate").forEach {
            assertEquals("native_failed", failureCode(RuntimeException(it)))
        }
        val cancelled = CancellationException("cancelled")
        assertSame(cancelled, assertThrows(CancellationException::class.java) { failureCode(cancelled) })
    }

    @Test fun cleanupCannotReplaceRecoveryOrCancellation() = runBlocking {
        val cleanup = RuntimeException("Rust error: native_shutdown_failed")
        val lost = RuntimeException("Rust error: runtime_lost")
        val actual = try { preservingCleanup(cleanup = { throw cleanup }) { throw lost }; null } catch (e: Exception) { e }
        assertSame(lost, actual); assertTrue(retryable(failureCode(actual!!))); assertSame(cleanup, actual.suppressed.single())
        val cancelled = CancellationException("stop")
        val stop = try { preservingCleanup(cleanup = { throw cleanup }) { throw cancelled }; null } catch (e: Exception) { e }
        assertSame(cancelled, stop)
        val standalone = try { preservingCleanup(cleanup = { throw cleanup }) { "ok" }; null } catch (e: Exception) { e }
        assertSame(cleanup, standalone)
        listOf("native_internal", "native_budget_exceeded", "native_overloaded", "invalid_native_request").forEach { assertFalse(retryable(it)) }
    }

    @Test fun durableJournalRestoresWallTimesAndRejectsRawText() = runBlocking {
        val time = 1791410000123L
        val stored = JournalFile(entries = listOf(JournalEntry(time, "runtime_lost")))
        val bytes = java.io.ByteArrayOutputStream()
        JournalSerializer.writeTo(stored, bytes)
        val restored = JournalSerializer.readFrom(ByteArrayInputStream(bytes.toByteArray()))
        assertEquals(stored, restored)
        assertEquals(listOf(JournalEntry(time, "runtime_lost"), JournalEntry(time + 10, "process_started")), restoreJournal(restored.entries, listOf(JournalEntry(time + 10, "process_started"))))
        val poisoned = """{"schema":1,"entries":[{"time":1,"text":"privatepassword"}]}"""
        try { JournalSerializer.readFrom(ByteArrayInputStream(poisoned.toByteArray())); fail("Raw journal text admitted") } catch (_: ClientFailure) { }
        val bounded = restoreJournal(List(200) { JournalEntry(time + it, "connected") }, listOf(JournalEntry(time + 500, "process_started")))
        assertEquals(200, bounded.size); assertEquals(time + 1, bounded.first().time)
    }

    @Test fun endpointAndSettingsPreserveEnrollmentContract() {
        assertEquals(Endpoint("2001:db8::1", 4222), Endpoint.parse("[2001:db8::1]:4222"))
        assertEquals(4222, Endpoint.parse("example.org:4222").port)
        listOf("http://example.org:4222", "example.org", "example.org:0", "x:70000", "x:4222/path").forEach { address ->
            assertThrows(ClientFailure::class.java) { Endpoint.parse(address) }
        }
        val settings = Settings(endpoint = "example.org:4222", login = "a".repeat(64), password = SealedPassword("iv", "cipher"), device = "a".repeat(32))
        settings.validate(true)
        assertThrows(ClientFailure::class.java) { settings.copy(login = "a.b").validate(true) }
        assertThrows(ClientFailure::class.java) { settings.copy(login = "__skvoz_server").validate(true) }
        assertThrows(ClientFailure::class.java) { settings.copy(schema = 2).validate() }
        assertThrows(ClientFailure::class.java) { settings.copy(mode = Mode.VPN, appPolicy = AppPolicy.INCLUDE).validate(true) }
    }
    @Test fun shortReadsAndInputOverflowAreExplicit() {
        val stream = object : ByteArrayInputStream(ByteArray(31) { it.toByte() }) {
            override fun read(bytes: ByteArray, offset: Int, length: Int) = super.read(bytes, offset, minOf(3, length))
        }
        assertArrayEquals(ByteArray(31) { it.toByte() }, stream.readBounded(31))
        assertThrows(ClientFailure::class.java) { ByteArrayInputStream(ByteArray(32)).readBounded(31) }
    }
    @Test fun journalIsBoundedAndAdmissionContainsOnlyIdentifiers() {
        var values = emptyList<JournalEntry>()
        repeat(250) { values = journal(values, it.toLong(), "connected") }
        assertEquals(200, values.size); assertEquals("connected", values.last().text)
        assertEquals("unknown_event", journal(values, 251, "private_token_or_arbitrary_exception").last().text)
        assertEquals("50", values.first().time.toString())
    }
    @Test fun pendingStopWaitsForUnwindBeforeReplacementAndLateCallbackIsIsolated() = runBlocking {
        Executors.newSingleThreadExecutor().asCoroutineDispatcher().use { dispatcher ->
            val entered = CountDownLatch(1); val release = CountDownLatch(1)
            val cancelled = mutableListOf<Long>(); var active = 0; var cleaned = false
            val oldSignals = RunSignals { token -> synchronized(cancelled) { cancelled.add(token) }; release.countDown() }
            oldSignals.enrollment.set(17)
            val pending = launch(dispatcher) {
                try { cancellableNative(17, { oldSignals.changed() }) {
                    active++; entered.countDown(); check(release.await(3, TimeUnit.SECONDS)); "old"
                } } finally { active--; cleaned = true; oldSignals.retire() }
            }
            check(entered.await(3, TimeUnit.SECONDS))
            pending.cancelAndJoin()
            assertTrue(cleaned); assertEquals(0, active); assertTrue(17L in cancelled)
            val fresh = RunSignals { error("Fresh enrollment cancelled by old callback") }
            fresh.enrollment.set(19)
            oldSignals.changed()
            assertEquals(0L, fresh.network.get()); assertEquals(19L, fresh.enrollment.get())
            assertEquals("new", withContext(dispatcher) { cancellableNative(19, { error("Unexpected cancel") }) { assertEquals(0, active); "new" } })
        }
    }
}
