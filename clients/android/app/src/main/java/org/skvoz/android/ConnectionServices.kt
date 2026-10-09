package org.skvoz.android

import android.app.*
import android.content.Intent
import android.content.pm.ServiceInfo
import android.net.VpnService
import android.os.Build
import android.os.IBinder
import android.os.ParcelFileDescriptor
import android.os.PowerManager
import android.os.SystemClock
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import android.provider.Settings as AndroidSettings
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.combine
import kotlinx.serialization.json.*

internal const val ACTION_START = "org.skvoz.android.START"
internal const val ACTION_STOP = "org.skvoz.android.STOP"
internal const val ACTION_SHOW_CONNECTION = "org.skvoz.android.SHOW_CONNECTION"
private const val CHANNEL = "connection"

internal class Foreground(private val service: Service, private val owner: ConnectionOwner) {
    private val notificationId = if (owner.mode == Mode.VPN) 2 else 1
    private var observing = false
    private val cadence = NotificationCadence()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private val app get() = service.application as SkvozApplication
    fun start() {
        val manager = service.getSystemService(NotificationManager::class.java)
        manager.createNotificationChannel(NotificationChannel(CHANNEL, "Соединение", NotificationManager.IMPORTANCE_DEFAULT).apply {
            setSound(null, null); enableVibration(false); enableLights(false); setShowBadge(false)
        })
        val type = if (Build.VERSION.SDK_INT < 34) 0 else if (owner.mode == Mode.VPN) ServiceInfo.FOREGROUND_SERVICE_TYPE_SYSTEM_EXEMPTED else ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE
        service.startForeground(notificationId, notification(ConnectionState(phase = "preparing", mode = owner.mode)), type)
        if (!observing) { observing = true; scope.launch { combine(app.connections.state, app.displayPreferences.state) { state, preferences -> state to preferences.format }.collect { (state, format) ->
            if (state.mode != owner.mode) return@collect
            val interactive = service.getSystemService(PowerManager::class.java).isInteractive
            val allowed = interactive && NotificationManagerCompat.from(service).areNotificationsEnabled() && manager.getNotificationChannel(CHANNEL)?.importance != NotificationManager.IMPORTANCE_NONE
            if (cadence.shouldPublish(notificationContent(state, format), SystemClock.elapsedRealtime(), interactive, allowed))
                manager.notify(notificationId, notification(state, format))
        } } }
    }
    private fun notification(state: ConnectionState, format: SpeedFormat = app.displayPreferences.state.value.format): Notification {
        val content = notificationContent(state, format)
        val open = PendingIntent.getActivity(service, 0, Intent(service, MainActivity::class.java).setAction(ACTION_SHOW_CONNECTION)
            .addFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_SINGLE_TOP), PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT)
        val action = if (owner.alwaysOn) PendingIntent.getActivity(service, 1, Intent(AndroidSettings.ACTION_VPN_SETTINGS), PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT)
            else PendingIntent.getService(service, 1, Intent(service, service.javaClass).setAction(ACTION_STOP), PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT)
        return NotificationCompat.Builder(service, CHANNEL).setSmallIcon(R.drawable.ic_connection)
            .setContentTitle(content.title).setContentText(content.status).setContentIntent(open).setOngoing(true)
            .setStyle(NotificationCompat.BigTextStyle().bigText(content.expanded))
            .setOnlyAlertOnce(true).setSilent(true).setShowWhen(false).setVisibility(NotificationCompat.VISIBILITY_PUBLIC)
            .setForegroundServiceBehavior(NotificationCompat.FOREGROUND_SERVICE_IMMEDIATE).setCategory(NotificationCompat.CATEGORY_SERVICE)
            .addAction(0, if (owner.alwaysOn) "Настройки ВПН" else "Отключиться", action).build()
    }
    fun close() { scope.cancel(); service.stopForeground(Service.STOP_FOREGROUND_REMOVE) }
}

// API 31 base covers denied background starts and API 34 missing/invalid types.
private fun startConnection(service: Service, owner: ConnectionOwner, foreground: Foreground, controller: ConnectionController): Boolean {
    try { foreground.start(); controller.start(owner); return true }
    catch (_: SecurityException) { }
    catch (_: ServiceStartNotAllowedException) { }
    catch (_: IllegalArgumentException) { }
    controller.stop(owner, "foreground_start_denied")
    service.stopSelf()
    return false
}

class ProxyService : Service(), ConnectionOwner {
    override val mode = Mode.PROXY
    override val alwaysOn = false
    override val lockdown = false
    private lateinit var foreground: Foreground
    private val controller get() = (application as SkvozApplication).connections
    override fun onCreate() { super.onCreate(); foreground = Foreground(this, this) }
    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == ACTION_STOP) { controller.record("user_stop"); controller.stop(this); return START_NOT_STICKY }
        if (intent?.action != ACTION_START) { stopSelf(); return START_NOT_STICKY }
        controller.record("service_started")
        startConnection(this, this, foreground, controller)
        return START_NOT_STICKY
    }
    override fun establish(config: JsonObject, settings: Settings): ParcelFileDescriptor = throw ClientFailure("invalid_state")
    override fun onBind(intent: Intent?): IBinder? = null
    override fun finish() { stopSelf() }
    override fun onDestroy() { controller.record("service_destroyed"); controller.stop(this); foreground.close(); super.onDestroy() }
}

class TunnelService : VpnService(), ConnectionOwner {
    override val mode = Mode.VPN
    override val alwaysOn get() = isAlwaysOn
    override val lockdown get() = isLockdownEnabled
    private lateinit var foreground: Foreground
    private val controller get() = (application as SkvozApplication).connections
    override fun onCreate() { super.onCreate(); foreground = Foreground(this, this) }
    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == ACTION_STOP) {
            if (!isAlwaysOn) { controller.record("user_stop"); controller.stop(this) }
            return START_NOT_STICKY
        }
        if (prepare(this) != null) { controller.report("vpn_permission_required"); stopSelf(); return START_NOT_STICKY }
        controller.record(if (intent == null) "service_restarted" else "service_started")
        if (!startConnection(this, this, foreground, controller)) return START_NOT_STICKY
        return START_STICKY
    }
    override fun establish(config: JsonObject, settings: Settings): ParcelFileDescriptor {
        settings.validate(true)
        if (prepare(this) != null) throw ClientFailure("vpn_permission_required")
        val families = config["families"]!!.jsonArray.map { it.jsonPrimitive.int }
        val mtu = config["mtu"]!!.jsonPrimitive.int
        if (families.isEmpty() || families.any { it !in setOf(4, 6) } || mtu !in 576..1500 || 6 in families && mtu < 1280)
            throw ClientFailure("invalid_configuration")
        val builder = Builder().setSession("SKVOZ").setMtu(mtu).setBlocking(false).setMetered(true)
        val selected = settings.packages - packageName
        if (settings.appPolicy == AppPolicy.INCLUDE) {
            if (selected.isEmpty()) throw ClientFailure("empty_app_selection")
            selected.forEach { name -> requireInstalled(name); builder.addAllowedApplication(name) }
        } else {
            (selected + packageName).forEach { name -> requireInstalled(name); builder.addDisallowedApplication(name) }
        }
        fun prefix(value: JsonElement, apply: (String, Int) -> Unit) {
            val text = value.jsonPrimitive.content
            val parts = text.split('/')
            if (parts.size != 2) throw ClientFailure("invalid_configuration")
            val family = if (':' in parts[0]) 6 else 4
            val length = parts[1].toIntOrNull() ?: throw ClientFailure("invalid_configuration")
            if (family !in families || length !in 0..(if (family == 6) 128 else 32) || !parts[0].matches(Regex("[0-9a-fA-F:.]+"))) throw ClientFailure("invalid_configuration")
            apply(parts[0], length)
        }
        val addresses = config["source_grants"]!!.jsonArray
        val routes = config["routes"]!!.jsonArray
        val dns = config["dns_servers"]!!.jsonArray
        if (addresses.size !in 1..8 || routes.size !in 1..32 || dns.size !in 1..4) throw ClientFailure("invalid_configuration")
        addresses.forEach { prefix(it) { address, length -> builder.addAddress(address, length) } }
        routes.forEach { prefix(it) { address, length -> builder.addRoute(address, length) } }
        dns.forEach {
            val address = it.jsonPrimitive.content
            if (!address.matches(Regex("[0-9a-fA-F:.]+")) || (if (':' in address) 6 else 4) !in families) throw ClientFailure("invalid_configuration")
            builder.addDnsServer(address)
        }
        builder.setConfigureIntent(PendingIntent.getActivity(this, 0, Intent(this, MainActivity::class.java), PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT))
        return builder.establish() ?: throw ClientFailure("vpn_establish_failed")
    }
    private fun requireInstalled(name: String) {
        try { packageManager.getApplicationInfo(name, 0) }
        catch (_: android.content.pm.PackageManager.NameNotFoundException) { throw ClientFailure("selected_app_missing") }
    }
    override fun onRevoke() { controller.record("vpn_revoked"); controller.stop(this); super.onRevoke() }
    override fun finish() { stopSelf() }
    override fun onDestroy() { controller.record("service_destroyed"); controller.stop(this); foreground.close(); super.onDestroy() }
}
internal fun phaseText(phase: String): String = when (phase) {
    "preparing" -> "Подготовка"; "enrolling" -> "Проверка учётной записи"; "connecting" -> "Подключение"
    "connected" -> "Подключено"; "reconnecting" -> "Переподключение"; "stopping" -> "Отключение"
    "error" -> "Ошибка подключения"; else -> "Отключено"
}
