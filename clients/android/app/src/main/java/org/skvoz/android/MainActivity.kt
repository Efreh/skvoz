package org.skvoz.android

import android.Manifest
import android.content.Intent
import android.content.ActivityNotFoundException
import android.net.Uri
import android.net.VpnService
import android.os.Build
import android.os.Bundle
import android.provider.Settings as AndroidSettings
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import androidx.lifecycle.Lifecycle
import kotlinx.coroutines.launch

class MainActivity : ComponentActivity() {
    private val model by lazy { ViewModelProvider(this, object : ViewModelProvider.Factory {
        override fun <T : ViewModel> create(modelClass: Class<T>): T {
            @Suppress("UNCHECKED_CAST") return MainViewModel(application as SkvozApplication) as T
        }
    })[MainViewModel::class.java] }
    private val consent = registerForActivityResult(ActivityResultContracts.StartActivityForResult()) { result ->
        val pending = model.permissionResult("waiting_vpn")
        if (result.resultCode == RESULT_OK && pending?.mode == Mode.VPN && model.revision == pending.revision) model.start(Mode.VPN, pending.revision)
        else if (result.resultCode != RESULT_OK) model.error("vpn_permission_required")
        model.finishStartup()
    }
    private val notificationPermission = registerForActivityResult(ActivityResultContracts.RequestPermission()) { allowed ->
        if (model.permissionResult("waiting_notifications") != null) {
            if (!allowed) model.error("notifications_denied")
            model.startupStage("vpn")
        }
    }
    private val importCa = registerForActivityResult(ActivityResultContracts.OpenDocument()) { uri -> uri?.let(model::importCa) }
    override fun onStart() { super.onStart(); model.diagnosticVisibility(true) }
    override fun onStop() { model.diagnosticVisibility(false); super.onStop() }
    override fun onResume() { super.onResume(); model.refreshPowerState() }
    private fun batterySettings() {
        val action = if (model.editor.value.batteryExempt == true) AndroidSettings.ACTION_IGNORE_BATTERY_OPTIMIZATION_SETTINGS
            else AndroidSettings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS
        val request = Intent(action).apply { if (action == AndroidSettings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS) data = Uri.parse("package:$packageName") }
        try { startActivity(request) }
        catch (_: ActivityNotFoundException) { model.error("battery_settings_unavailable") }
        catch (_: SecurityException) { model.error("battery_settings_unavailable") }
    }
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                model.startup.collect { pending ->
                    if (pending == null) return@collect
                    if (model.revision != pending.revision) { model.finishStartup(); return@collect }
                    when (pending.stage) {
                        "notifications" -> {
                            if (Build.VERSION.SDK_INT >= 33 && checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != android.content.pm.PackageManager.PERMISSION_GRANTED) {
                                model.startupStage("waiting_notifications"); notificationPermission.launch(Manifest.permission.POST_NOTIFICATIONS)
                            } else model.startupStage("vpn")
                        }
                        "vpn" -> {
                            val request = if (pending.mode == Mode.VPN) VpnService.prepare(this@MainActivity) else null
                            if (request != null) { model.startupStage("waiting_vpn"); consent.launch(request) }
                            else { model.start(pending.mode, pending.revision); model.finishStartup() }
                        }
                    }
                }
            }
        }
        setContent { ConnectionScreen(model, onConnect = {
            model.save(model::beginStartup)
        }, onImport = { importCa.launch(arrayOf("application/x-pem-file", "application/x-x509-ca-cert", "application/pkix-cert", "text/plain", "application/octet-stream")) },
            onVpnSettings = { startActivity(Intent(AndroidSettings.ACTION_VPN_SETTINGS)) }, onBatterySettings = ::batterySettings) }
    }
}
