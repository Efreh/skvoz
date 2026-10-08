package org.skvoz.android

import android.net.ConnectivityManager
import android.os.ParcelFileDescriptor
import android.os.SystemClock
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.serialization.json.*

internal interface ConnectionOwner {
    val mode: Mode
    val alwaysOn: Boolean
    val lockdown: Boolean
    fun establish(config: JsonObject, settings: Settings): ParcelFileDescriptor
    fun finish()
}
internal class ConnectionController(private val app: SkvozApplication) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private val mutable = MutableStateFlow(ConnectionState())
    val state = mutable.asStateFlow()
    private val detailedControl = DiagnosticControl()
    private val diagnosticMutable = MutableStateFlow(DiagnosticState())
    val diagnostics = diagnosticMutable.asStateFlow()
    fun detailedDiagnostics(enabled: Boolean) {
        detailedControl.request(enabled) { diagnosticMutable.update { it.copy(detail = null, code = null) } }
    }
    private val commands = Channel<ConnectionOwner?>(Channel.CONFLATED)
    private val lock = Any()
    private var desired: ConnectionOwner? = null
    private var stopError: String? = null
    private var worker: Job? = null
    init {
        scope.launch { app.journal.entries.collect { entries -> mutable.update { it.copy(journal = entries) } } }
        scope.launch {
            var previous: ConnectionOwner? = null
            for (owner in commands) {
                worker?.cancelAndJoin(); worker = null
                if (synchronized(lock) { desired !== owner }) continue
                if (previous !== owner) previous?.finish()
                previous = owner
                if (owner == null) {
                    val error = synchronized(lock) { stopError }
                    record(error ?: "disconnected")
                    diagnosticMutable.value = DiagnosticState()
                    mutable.update { it.copy(phase = if (error == null) "disconnected" else "error", error = error, alwaysOn = false, lockdown = false, upRate = 0, downRate = 0) }
                } else worker = scope.launch(Dispatchers.IO) { run(owner) }
            }
        }
    }
    fun start(owner: ConnectionOwner) = synchronized(lock) {
        desired = owner; stopError = null
        cancelCurrent()
        commands.trySend(owner)
    }
    fun stop(owner: ConnectionOwner? = null, error: String? = null) = synchronized(lock) {
        if (owner != null && desired !== owner && !(error != null && desired == null)) {
            // A denied replacement must be visible without retiring another owner.
            if (error != null) { record(error); mutable.update { it.rejectedForeground(System.currentTimeMillis()) } }
            return@synchronized
        }
        desired = null; stopError = error; cancelCurrent(); commands.trySend(null)
        if (error != null) report(error)
        mutable.update { if (it.active) it.copy(phase = "stopping") else it }
    }
    private fun cancelCurrent() {
        worker?.cancel()
    }
    fun record(code: String) { app.journal.append(code) }
    fun report(code: String) { record(code); mutable.update { it.copy(phase = "error", error = code) } }
    private fun transition(phase: String, owner: ConnectionOwner, error: String? = null) {
        record(error ?: phase)
        diagnosticMutable.value = DiagnosticState()
        mutable.update { it.copy(phase = phase, mode = owner.mode, error = error, alwaysOn = owner.alwaysOn,
            lockdown = owner.lockdown, upRate = 0, downRate = 0) }
    }
    private suspend fun run(owner: ConnectionOwner) {
        val signals = RunSignals(NativeBridge::cancelEnrollment)
        val monitor = NetworkMonitor(app.getSystemService(ConnectivityManager::class.java), signals)
        val started = SystemClock.elapsedRealtime()
        var attempt = 0
        try {
            monitor.register()
            mutable.update { it.copy(uploaded = 0, downloaded = 0, started = started, elapsed = 0, upRate = 0, downRate = 0) }
            while (currentCoroutineContext().isActive) {
                try {
                    transition(if (attempt == 0) "preparing" else "reconnecting", owner)
                    runAttempt(owner, signals, started) { attempt = 0 }
                } catch (cancel: CancellationException) { throw cancel }
                  catch (error: Exception) {
                    val code = failureCode(error)
                    if (!retryable(code)) { transition("error", owner, code); return }
                    attempt++; transition("reconnecting", owner, code)
                }
                delay(minOf(15000L, 1000L shl minOf(attempt - 1, 4)))
            }
        } catch (cancel: CancellationException) { throw cancel }
          catch (error: Exception) { transition("error", owner, failureCode(error)) }
        finally {
            if (!currentCoroutineContext().isActive) record("connection_cancelled")
            monitor.close()
            if (mutable.value.phase == "error" && !owner.alwaysOn) finishFailedOwner(owner)
        }
    }
    private suspend fun finishFailedOwner(owner: ConnectionOwner) = withContext(NonCancellable + Dispatchers.Main) {
        synchronized(lock) { if (desired === owner) { desired = null; owner.finish() } }
    }
    private suspend fun runAttempt(owner: ConnectionOwner, signals: RunSignals, started: Long, onConnected: () -> Unit) {
        if (synchronized(lock) { desired !== owner }) throw CancellationException("Superseded owner")
        val network = signals.network.get()
        val settings = app.profiles.read()
        settings.validate(true)
        if (settings.mode != owner.mode) throw ClientFailure("mode_mismatch")
        val config = enrollProfile(settings, signals, owner)
        currentCoroutineContext().ensureActive()
        if (signals.network.get() != network) throw ClientFailure("network_unavailable")
        transition("connecting", owner)
        val session = NativeSession(config, counters(owner, started), detailedControl::current,
            { request, snapshot, code -> detailedControl.publish(request) { diagnosticMutable.update { it.copy(detail = snapshot, code = code) } } }) { record("native_shutdown_failed") }
        preservingCleanup(cleanup = { withContext(NonCancellable) { session.close() } }) {
            session.hello()
            if (owner.mode == Mode.PROXY) setupProxy(session, settings) else setupVpn(session, owner, settings)
            transition("connected", owner); onConnected()
            while (currentCoroutineContext().isActive) {
                if (signals.network.get() != network) throw ClientFailure("network_unavailable")
                session.pump()
            }
        }
    }
    private suspend fun enrollProfile(settings: Settings, signals: RunSignals, owner: ConnectionOwner): String {
        val endpoint = Endpoint.parse(settings.endpoint)
        val password = app.profiles.decrypt(settings)
        val ca = app.trust.path(settings.customCa)
        val profile = buildJsonObject {
            put("host", endpoint.host); put("port", endpoint.port); put("username", settings.login)
            put("password", password); put("ca_file", ca); put("device", settings.device)
        }.toString()
        transition("enrolling", owner)
        val token = NativeBridge.cancellationToken()
        if (token <= 0) throw ClientFailure("native_request_exhausted")
        signals.enrollment.set(token)
        try {
            currentCoroutineContext().ensureActive()
            return cancellableNative(token, NativeBridge::cancelEnrollment) { NativeBridge.enroll(profile, token) }
        } finally { signals.enrollment.compareAndSet(token, 0) }
    }
    private fun counters(owner: ConnectionOwner, started: Long): (JsonObject) -> Unit {
        val baseUp = mutable.value.uploaded; val baseDown = mutable.value.downloaded
        var lastUp = 0L; var lastDown = 0L; var lastTime = SystemClock.elapsedRealtime()
        return { event ->
            if (event["event"]?.jsonPrimitive?.content == "STATS") {
                val counters = event["data"]!!.jsonObject["counters"]!!.jsonObject
                try { val basic = basicMetrics(counters); diagnosticMutable.update { it.copy(basic = basic) } }
                catch (_: Exception) { diagnosticMutable.update { it.copy(code = "diagnostics_unavailable") } }
                val up = counters["uploaded"]!!.jsonPrimitive.long
                val down = counters["downloaded"]!!.jsonPrimitive.long
                if (up < lastUp || down < lastDown) throw ClientFailure("native_invalid_counters")
                val now = SystemClock.elapsedRealtime(); val span = (now - lastTime).coerceAtLeast(1)
                mutable.update { it.copy(uploaded = baseUp + up, downloaded = baseDown + down,
                    upRate = (up - lastUp) * 1000 / span, downRate = (down - lastDown) * 1000 / span,
                    elapsed = now - started, alwaysOn = owner.alwaysOn, lockdown = owner.lockdown) }
                lastUp = up; lastDown = down; lastTime = now
            }
        }
    }
    private suspend fun setupProxy(session: NativeSession, settings: Settings) {
        val result = session.call("START_PROXY", buildJsonObject {
            put("http_bind", "127.0.0.1:${settings.httpPort}"); put("socks_bind", "127.0.0.1:${settings.socksPort}")
        }).jsonObject
        if (result["http"]?.jsonPrimitive?.content != "http://127.0.0.1:${settings.httpPort}" ||
            result["socks"]?.jsonPrimitive?.content != "socks5://127.0.0.1:${settings.socksPort}") throw ClientFailure("native_invalid_response")
    }
    private suspend fun setupVpn(session: NativeSession, owner: ConnectionOwner, settings: Settings) {
        val handle = session.call("START_IP", buildJsonObject {
            put("families", buildJsonArray { add(4); add(6) }); put("family_policy", "auto"); put("max_mtu", 1500); put("channels", 1)
        }).jsonObject["handle"] ?: throw ClientFailure("native_invalid_response")
        val configured = session.event("CONFIGURED")
        if (configured["handle"] != handle) throw ClientFailure("native_invalid_event")
        val vpn = configured["config"]?.jsonObject ?: throw ClientFailure("invalid_configuration")
        val mtu = vpn["mtu"]!!.jsonPrimitive.int
        val descriptor = owner.establish(vpn, settings)
        try {
            val interfaceName = NativeBridge.tunName(descriptor.fd, mtu)
            val attached = session.call("ATTACH_IP", buildJsonObject {
                put("handle", handle); put("interface", interfaceName); put("mtu", mtu)
            }, descriptor.fd)
            if (attached.jsonObject["handle"] != handle) throw ClientFailure("native_invalid_response")
        } finally { descriptor.close() }
        if (session.call("LOCAL_READY", buildJsonObject { put("handle", handle) }).jsonObject["handle"] != handle ||
            session.event("ACTIVE")["handle"] != handle) throw ClientFailure("native_invalid_response")
    }
}
