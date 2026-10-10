package org.skvoz.android

import android.os.SystemClock
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.serialization.json.*
import java.io.Closeable

internal class NativeSession(config: String, private val onEvent: (JsonObject) -> Unit, private val detailedWanted: () -> Long, private val onDiagnostic: (Long, DiagnosticSnapshot?, String?) -> Unit, private val cleanupFailed: () -> Unit) : Closeable {
    private var handle = NativeBridge.start(config)
    private var nextId = 1
    private var sequence = 0L
    private var eventBytes = 0
    private val events = ArrayDeque<Pair<JsonObject, Int>>()
    private var ready = false
    private val diagnosticCollector = DiagnosticCollector(detailedWanted,
        { enabled -> NativeBridge.diagnostics(handle, enabled) }, onDiagnostic, SystemClock::elapsedRealtime)
    suspend fun call(op: String, args: JsonObject = buildJsonObject {}, fd: Int = -1): JsonElement {
        if (nextId == Int.MAX_VALUE) throw ClientFailure("native_request_exhausted")
        val id = nextId++
        val request = buildJsonObject {
            put("v", 1); put("id", id); put("op", op); put("args", args); put("fd_count", if (fd >= 0) 1 else 0)
        }.toString()
        if (request.encodeToByteArray().size > 32768) throw ClientFailure("native_budget_exceeded")
        NativeBridge.request(handle, request, fd)
        val deadline = SystemClock.elapsedRealtime() + 20000
        while (SystemClock.elapsedRealtime() < deadline) {
            val value = receive() ?: continue
            if (value["id"]?.jsonPrimitive?.intOrNull != id) throw ClientFailure("native_invalid_response")
            if (value["error"] != JsonNull) throw ClientFailure(apiFailureCode(value["error"]?.jsonPrimitive?.content ?: "native_invalid_response"))
            return value["result"] ?: throw ClientFailure("native_invalid_response")
        }
        throw ClientFailure("timeout")
    }
    suspend fun event(name: String, timeout: Long = 20000): JsonObject {
        val deadline = SystemClock.elapsedRealtime() + timeout
        while (SystemClock.elapsedRealtime() < deadline) {
            val index = events.indexOfFirst { it.first["event"]?.jsonPrimitive?.content == name }
            if (index >= 0) {
                val pair = events.removeAt(index); eventBytes -= pair.second
                return pair.first["data"]?.jsonObject ?: throw ClientFailure("native_invalid_event")
            }
            if (receive() != null) throw ClientFailure("native_unexpected_response")
        }
        throw ClientFailure("timeout")
    }
    suspend fun hello() {
        val hello = call("HELLO", buildJsonObject { put("api", 1); put("network", 5) }).jsonObject
        val cap = hello["capabilities"]?.jsonObject ?: throw ClientFailure("version_mismatch")
        if (hello["api"]?.jsonPrimitive?.intOrNull != 1 || hello["network"]?.jsonPrimitive?.intOrNull != 5 ||
            hello["role"]?.jsonPrimitive?.content != "client" ||
            cap["profiles"] != buildJsonArray { add("tcp"); add("ip") } ||
            cap["families"] != buildJsonArray { add(4); add(6) } ||
            cap["max_mtu"]?.jsonPrimitive?.intOrNull != 1500 || cap["max_channels"]?.jsonPrimitive?.intOrNull != 1)
            throw ClientFailure("version_mismatch")
        while (!ready) {
            val state = event("RUNTIME_STATE")
            if (state["state"]?.jsonPrimitive?.content == "ready") ready = true
            else if (state["error"] != JsonNull) throw ClientFailure("network_unavailable")
        }
    }
    suspend fun pump() {
        if (receive() != null) throw ClientFailure("native_unexpected_response")
        // Operational events are consumed immediately; setup events cannot grow forever.
        events.clear(); eventBytes = 0
    }
    private suspend fun receive(): JsonObject? {
        currentCoroutineContext().ensureActive()
        diagnosticCollector.collect()
        val raw = NativeBridge.poll(handle) ?: return null
        val byteCount = raw.encodeToByteArray().size
        if (byteCount > 32768) throw ClientFailure("native_budget_exceeded")
        val value = try { wireJson.parseToJsonElement(raw).jsonObject } catch (_: Exception) { throw ClientFailure("native_invalid_response") }
        if (value["v"]?.jsonPrimitive?.intOrNull != 1 || value["fd_count"]?.jsonPrimitive?.intOrNull != 0) throw ClientFailure("version_mismatch")
        if (value.containsKey("id")) return value
        val seq = value["seq"]?.jsonPrimitive?.longOrNull ?: throw ClientFailure("native_invalid_event")
        if (seq <= sequence) throw ClientFailure("native_invalid_event")
        sequence = seq
        val name = value["event"]?.jsonPrimitive?.content ?: throw ClientFailure("native_invalid_event")
        val data = value["data"]?.jsonObject ?: throw ClientFailure("native_invalid_event")
        if (name in setOf("CLOSED", "ERROR") || name == "RUNTIME_STATE" && ready && data["state"]?.jsonPrimitive?.content != "ready") throw ClientFailure("runtime_lost")
        onEvent(value)
        if (name !in setOf("STATS", "REQUEST")) {
            if (events.size >= 64 || eventBytes + byteCount > 65536) throw ClientFailure("native_event_overflow")
            events.addLast(value to byteCount); eventBytes += byteCount
        }
        return null
    }
    override fun close() {
        val old = handle; handle = 0
        onDiagnostic(detailedWanted(), null, null)
        try { if (old != 0L) NativeBridge.stop(old) }
        catch (error: Exception) { cleanupFailed(); throw error }
        finally { events.clear(); eventBytes = 0 }
    }
}
