package org.skvoz.android

import android.os.Debug
import android.os.Process
import android.os.SystemClock
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.collectLatest

internal data class CpuReading(val time: Long, val cpu: Long)
internal fun cpuPercent(previous: CpuReading, current: CpuReading): Double? {
    val elapsed = current.time - previous.time
    val used = current.cpu - previous.cpu
    return if (elapsed <= 0 || used < 0) null else used.toDouble() / elapsed * 100.0
}
internal data class ResourceState(val cpu: Double? = null, val pssBytes: Long? = null, val enabled: Boolean = false, val pending: Boolean = false)
internal fun cpuBasisPoints(cpu: Double?): ULong = cpu?.takeIf { it.isFinite() && it >= 0 }?.let { (it * 100).toULong() } ?: 0uL
internal fun cpuLabel(cpu: Double?): String = cpu?.takeIf { it.isFinite() && it >= 0 }?.let { "%.1f %%".format(it) } ?: "—"
internal data class ResourceRanges(val cpu: ULong = 10000uL, val ram: ULong = 1048576uL) {
    fun update(sample: ResourceState): ResourceRanges = if (!sample.enabled) ResourceRanges() else copy(
        cpu = expandedRange(cpu, cpuBasisPoints(sample.cpu)),
        ram = expandedRange(ram, (sample.pssBytes ?: 0).coerceAtLeast(0).toULong()),
    )
}

internal class AndroidResourceReader {
    fun cpu() = CpuReading(SystemClock.elapsedRealtime(), Process.getElapsedCpuTime())
    fun memory(): Long? = Debug.getPss().takeIf { it > 0 }?.times(1024)
}

// One cancellable IO collector; no sampler, clocks or memory reads while Home is hidden or the option is off.
internal class ResourceSampler(scope: CoroutineScope, private val cpu: () -> CpuReading, private val memory: () -> Long?,
    dispatcher: CoroutineDispatcher = Dispatchers.IO) {
    private val request = MutableStateFlow(0L)
    private val lock = Any()
    private val mutable = MutableStateFlow(ResourceState())
    val state = mutable.asStateFlow()
    init { scope.launch(dispatcher) { request.collectLatest { epoch ->
        val active = epoch.and(1L) != 0L
        synchronized(lock) { if (request.value == epoch) mutable.value = ResourceState(enabled = active, pending = active) }
        if (!active) return@collectLatest
        var previous: CpuReading? = null
        var ram: Long? = null
        var tick = 0
        while (currentCoroutineContext().isActive) {
            if (request.value != epoch) return@collectLatest
            val current = try { cpu() } catch (e: CancellationException) { throw e } catch (_: Exception) { null }
            if (request.value != epoch) return@collectLatest
            if (tick % 4 == 0) ram = try { memory()?.takeIf { it > 0 } } catch (e: CancellationException) { throw e } catch (_: Exception) { null }
            currentCoroutineContext().ensureActive()
            synchronized(lock) {
                if (request.value == epoch) mutable.value = ResourceState(previous?.let { old -> current?.let { cpuPercent(old, it) } }, ram, true, previous == null && current != null)
            }
            previous = current
            tick = (tick + 1) % 4
            delay(3000)
        }
    } } }
    fun visibility(visible: Boolean) = synchronized(lock) {
        val old = request.value
        if (old.and(1L) != (if (visible) 1L else 0L))
            request.value = ((old.ushr(1) + 1).shl(1)) or (if (visible) 1L else 0L)
        if (!visible) mutable.value = ResourceState()
    }
}
