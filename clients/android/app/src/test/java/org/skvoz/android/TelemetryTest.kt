package org.skvoz.android

import androidx.datastore.core.CorruptionException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.*
import kotlinx.coroutines.runBlocking
import org.junit.Assert.*
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.Locale

@OptIn(ExperimentalCoroutinesApi::class)
class TelemetryTest {
    @Test fun speedFormatsUseBinaryBytesDecimalBitsAndDoNotOverflow() {
        val previous = Locale.getDefault()
        Locale.setDefault(Locale.US)
        try {
            assertEquals("0 Б/с", speed(-1, SpeedFormat.BYTES))
            assertEquals("1.0 КиБ/с", speed(1024, SpeedFormat.BYTES))
            assertEquals("1.0 МиБ/с", speed(1048576, SpeedFormat.BYTES))
            assertEquals("1.0 ГиБ/с", speed(1073741824, SpeedFormat.BYTES))
            assertEquals("992 бит/с", speed(124, SpeedFormat.BITS))
            assertEquals("1.0 Кбит/с", speed(125, SpeedFormat.BITS))
            assertEquals("1.0 Мбит/с", speed(125000, SpeedFormat.BITS))
            assertEquals("1.0 Гбит/с", speed(125000000, SpeedFormat.BITS))
            assertFalse(speed(ULong.MAX_VALUE, SpeedFormat.BITS).startsWith("-"))
            val state = ConnectionState(phase = "connected", downRate = 125000, upRate = 125)
            assertTrue(notificationContent(state, SpeedFormat.BITS).expanded.contains("↓ 1.0 Мбит/с"))
            assertTrue(notificationContent(state, SpeedFormat.BITS).expanded.contains("↑ 1.0 Кбит/с"))
            val cadence = NotificationCadence()
            assertTrue(cadence.shouldPublish(notificationContent(state), 1000, true, true))
            assertFalse(cadence.shouldPublish(notificationContent(state, SpeedFormat.BITS), 1001, false, true))
            assertTrue(cadence.shouldPublish(notificationContent(state, SpeedFormat.BITS), 1001, true, true))
            assertEquals("1.0 МиБ", volume(1048576))
            assertEquals(5, litSegments(1024uL, 8192uL))
        } finally { Locale.setDefault(previous) }
    }

    @Test fun preferencesRoundTripWithoutProfileDataAndRejectUnknownFormats() = runBlocking {
        val output = ByteArrayOutputStream()
        DisplayPreferencesSerializer.writeTo(DisplayPreferences(speedFormat = SpeedFormat.BITS, showResources = true), output)
        assertEquals(SpeedFormat.BITS, DisplayPreferencesSerializer.readFrom(ByteArrayInputStream(output.toByteArray())).speedFormat)
        assertTrue(DisplayPreferencesSerializer.readFrom(ByteArrayInputStream(output.toByteArray())).showResources)
        assertFalse(DisplayPreferencesSerializer.defaultValue.showResources)
        assertFalse(DisplayPreferencesSerializer.readFrom(ByteArrayInputStream("{}".toByteArray())).showResources)
        assertEquals(SpeedFormat.BYTES, DisplayPreferencesSerializer.defaultValue.speedFormat)
        assertFalse(output.toString().contains("password"))
        listOf("{\"schema\":2}", "{\"speedFormat\":\"INVALID\"}", "x".repeat(1025)).forEach { invalid ->
            try { DisplayPreferencesSerializer.readFrom(ByteArrayInputStream(invalid.toByteArray())); fail("Invalid preferences accepted") }
            catch (_: CorruptionException) { }
        }
    }

    @Test fun processCpuUsesOneCoreAndRejectsClockAndCounterResets() {
        assertEquals(50.0, cpuPercent(CpuReading(1000, 100), CpuReading(4000, 1600))!!, 0.001)
        assertEquals(200.0, cpuPercent(CpuReading(0, 0), CpuReading(3000, 6000))!!, 0.001)
        assertNull(cpuPercent(CpuReading(1, 10), CpuReading(1, 11)))
        assertNull(cpuPercent(CpuReading(0, 100), CpuReading(3000, 0)))
    }

    @Test fun resourceScalesExpandAboveOneCoreKeepAbsoluteMemoryAndResetWhenOff() {
        val sample = ResourceState(cpu = 250.0, pssBytes = 150 * 1048576L, enabled = true)
        val ranges = ResourceRanges().update(sample)
        assertEquals(40000uL, ranges.cpu)
        assertEquals(256uL * 1048576uL, ranges.ram)
        assertEquals(25000uL, cpuBasisPoints(sample.cpu))
        assertEquals(25, litSegments(cpuBasisPoints(sample.cpu), ranges.cpu))
        assertEquals(ranges, ranges.update(sample.copy(cpu = 1.0, pssBytes = 1048576)))
        assertEquals(ResourceRanges(), ranges.update(ResourceState()))
        assertEquals(0uL, cpuBasisPoints(Double.NaN))
    }

    @Test fun samplerIsIdleWhenHiddenUsesSlowerMemoryAndResetsAfterRapidReopen() = runTest {
        var reads = 0; var memoryReads = 0
        val sampler = ResourceSampler(backgroundScope, { reads++; CpuReading(testScheduler.currentTime, testScheduler.currentTime / 2) },
            { memoryReads++; 65536L }, StandardTestDispatcher(testScheduler))
        runCurrent(); advanceTimeBy(60000); runCurrent()
        assertEquals(0, reads); assertEquals(0, memoryReads)
        sampler.visibility(true); runCurrent()
        assertNull(sampler.state.value.cpu); assertEquals(65536L, sampler.state.value.pssBytes)
        advanceTimeBy(12000); runCurrent()
        assertEquals(5, reads); assertEquals(2, memoryReads)
        assertEquals(50.0, sampler.state.value.cpu!!, 0.001)
        sampler.visibility(false); sampler.visibility(true); runCurrent()
        assertNull(sampler.state.value.cpu)
        sampler.visibility(false); runCurrent()
        val stoppedReads = reads
        advanceTimeBy(60000); runCurrent()
        assertEquals(stoppedReads, reads); assertEquals(ResourceState(), sampler.state.value)
    }

    @Test fun lateMemoryResultCannotPublishAfterExitAndFailuresRemainLocal() = runTest {
        lateinit var sampler: ResourceSampler
        sampler = ResourceSampler(backgroundScope, { CpuReading(0, 0) }, { sampler.visibility(false); 123L }, StandardTestDispatcher(testScheduler))
        sampler.visibility(true); runCurrent()
        assertEquals(ResourceState(), sampler.state.value)
        val failing = ResourceSampler(backgroundScope, { throw IllegalStateException("Unavailable") },
            { throw IllegalStateException("Unavailable") }, StandardTestDispatcher(testScheduler))
        failing.visibility(true); runCurrent(); advanceTimeBy(3000); runCurrent()
        assertEquals(ResourceState(enabled = true), failing.state.value)
        assertEquals("—", cpuLabel(Double.NaN))
    }
}
