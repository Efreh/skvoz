package org.skvoz.android

import android.os.Process
import android.os.SystemClock
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import java.util.Collections
import org.json.JSONObject
import java.util.concurrent.atomic.AtomicInteger
import androidx.lifecycle.ViewModelStore

@RunWith(AndroidJUnit4::class)
class TelemetryDeviceTest {
    private val context get() = InstrumentationRegistry.getInstrumentation().targetContext

    @Test fun realDisplayStorePersistsWithoutTouchingTheConnectionProfile() = runBlocking {
        val app = context.applicationContext as SkvozApplication
        val store = app.displayPreferences
        val original = withTimeout(10000) { store.state.first { it.value != null } }.value!!
        val profile = context.filesDir.resolve("profile.json")
        val before = profile.takeIf { it.exists() }?.readBytes()
        try {
            store.resources(false)
            store.select(SpeedFormat.BYTES)
            store.select(SpeedFormat.BITS)
            store.resources(true)
            assertEquals(SpeedFormat.BITS, store.state.value.format)
            val persisted = context.filesDir.resolve("display.json").inputStream().use { DisplayPreferencesSerializer.readFrom(it) }
            assertEquals(SpeedFormat.BITS, persisted.speedFormat)
            assertTrue(persisted.showResources)
            assertTrue("Connection profile changed", before?.contentEquals(profile.readBytes()) ?: !profile.exists())
        } finally { store.select(original.speedFormat); store.resources(original.showResources) }
    }

    @Test fun actualSamplerHasBoundedCostAndStopsAllReadsWhenHidden() = runBlocking {
        val reader = AndroidResourceReader()
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        val costs = Collections.synchronizedList(mutableListOf<Double>())
        val cpuReads = AtomicInteger(); val memoryReads = AtomicInteger()
        fun <T> measured(read: () -> T): T {
            val start = SystemClock.elapsedRealtimeNanos()
            return try { read() } finally { costs.add((SystemClock.elapsedRealtimeNanos() - start) / 1e6) }
        }
        val sampler = ResourceSampler(scope, { cpuReads.incrementAndGet(); measured(reader::cpu) }, { memoryReads.incrementAndGet(); measured(reader::memory) })
        suspend fun idleCost(): Double {
            val start = reader.cpu()
            delay(15000)
            return cpuPercent(start, reader.cpu())!!
        }
        try {
            val baseline = idleCost()
            assertEquals(0, cpuReads.get()); assertEquals(0, memoryReads.get())
            val start = reader.cpu()
            sampler.visibility(true)
            delay(61000)
            val activeCost = cpuPercent(start, reader.cpu())!!
            assertNotNull(sampler.state.value.cpu); assertTrue((sampler.state.value.pssBytes ?: 0) > 0)
            sampler.visibility(false)
            delay(500)
            val stoppedCpu = cpuReads.get(); val stoppedMemory = memoryReads.get()
            val after = idleCost()
            assertEquals(stoppedCpu, cpuReads.get()); assertEquals(stoppedMemory, memoryReads.get())
            assertEquals(ResourceState(), sampler.state.value)
            assertTrue(cpuReads.get() in 19..22); assertTrue(memoryReads.get() in 5..6)
            // The first control may still contain process startup/JIT; use the lower idle control conservatively.
            val overhead = (activeCost - minOf(baseline, after)).coerceAtLeast(0.0)
            val sorted = costs.toList().sorted()
            val p95 = sorted[((sorted.size - 1) * 0.95).toInt()]
            val report = JSONObject().put("baselineCpuPercent", baseline).put("activeCpuPercent", activeCost)
                .put("afterCpuPercent", after).put("additionalCpuPercent", overhead).put("apiP95Milliseconds", p95)
                .put("cpuReads", cpuReads.get()).put("memoryReads", memoryReads.get()).put("noReadsWhenHidden", true)
            context.filesDir.resolve("telemetry-test.json").writeText(report.toString(2))
            assertTrue("Sampler overhead exceeds one percent of one core", overhead <= 1.0)
            assertTrue("Resource API latency exceeds 50 ms at p95", p95 <= 50.0)
        } finally { sampler.visibility(false); scope.cancel() }
    }

    @Test fun resourcesRequireSavedOptionHomeAndVisibleActivity() = runBlocking {
        val instrumentation = InstrumentationRegistry.getInstrumentation()
        val app = context.applicationContext as SkvozApplication
        val preferences = app.displayPreferences
        val original = withTimeout(10000) { preferences.state.first { it.value != null } }.value!!
        val store = ViewModelStore()
        lateinit var model: MainViewModel
        fun onMain(action: () -> Unit) = instrumentation.runOnMainSync(action)
        preferences.resources(false)
        onMain { model = MainViewModel(app); store.put("telemetry", model) }
        try {
            onMain { model.diagnosticVisibility(true); model.resourceHome(true) }
            delay(3500)
            assertEquals(ResourceState(), model.resources.value)
            preferences.resources(true)
            val sample = withTimeout(15000) { model.resources.first { it.cpu != null && it.pssBytes != null } }
            assertTrue(sample.enabled)
            onMain { model.resourceHome(false); model.diagnosticPanel(true) }
            delay(3500)
            assertEquals(ResourceState(), model.resources.value)
            onMain { model.diagnosticPanel(false); model.resourceHome(true) }
            withTimeout(15000) { model.resources.first { it.cpu != null } }
            preferences.resources(false)
            withTimeout(5000) { model.resources.first { !it.enabled } }
            assertEquals(ResourceState(), model.resources.value)
            preferences.resources(true)
            withTimeout(15000) { model.resources.first { it.cpu != null } }
            onMain { model.diagnosticVisibility(false) }
            delay(4000)
            assertEquals(ResourceState(), model.resources.value)
        } finally {
            onMain { store.clear() }
            preferences.select(original.speedFormat); preferences.resources(original.showResources)
        }
    }
}
