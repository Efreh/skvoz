package org.skvoz.android

import android.content.Intent
import android.net.Uri
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.flow.combine
import java.util.concurrent.atomic.AtomicLong

internal data class SelectableApp(val name: String, val label: String, val missing: Boolean = false)
internal data class Editor(val settings: Settings = Settings(), val password: String = "", val loaded: Boolean = false,
    val busy: Boolean = false, val batteryExempt: Boolean? = null, val error: String? = null, val apps: List<SelectableApp> = emptyList(),
    val savedSettings: Settings = Settings(), val savedPassword: String = "", val httpPort: String = "18080", val socksPort: String = "18081",
    val fieldErrors: Map<ProfileField, String> = emptyMap(), val appsLoading: Boolean = false, val appsError: String? = null,
    val rejectedInput: Map<ProfileField, String> = emptyMap(),
) {
    val dirty get() = loaded && (settings != savedSettings || password != savedPassword ||
        httpPort != savedSettings.httpPort.toString() || socksPort != savedSettings.socksPort.toString())
    val hasProfile get() = savedSettings.password != null && savedSettings.endpoint.isNotEmpty()
}
internal data class PendingStartup(val mode: Mode, val revision: Long, val stage: String)
internal class MainViewModel(private val app: SkvozApplication) : ViewModel() {
    private val mutable = MutableStateFlow(Editor())
    private val pending = MutableStateFlow<PendingStartup?>(null)
    val startup = pending.asStateFlow()
    private val submittedMutable = MutableSharedFlow<Unit>(extraBufferCapacity = 1)
    val submitted = submittedMutable.asSharedFlow()
    private var permissionRequest: PendingStartup? = null
    fun beginStartup(mode: Mode) { if (permissionRequest != null) { error("permission_request_pending"); return }; pending.value = PendingStartup(mode, revision, "notifications") }
    fun startupStage(stage: String) {
        val value = pending.value ?: return
        pending.value = if (value.revision == revision) value.copy(stage = stage) else null
        if (stage.startsWith("waiting_") && pending.value != null) permissionRequest = pending.value
    }
    fun permissionResult(stage: String): PendingStartup? {
        val issued = permissionRequest ?: return null
        if (issued.stage != stage) return null
        permissionRequest = null
        val current = pending.value
        return current?.takeIf { it == issued && it.revision == revision }
    }
    fun finishStartup() { pending.value = null }
    val editor = mutable.asStateFlow()
    private val generation = AtomicLong(1)
    val revision get() = generation.get()
    val connection = app.connections.state
    val diagnostics = app.connections.diagnostics
    private val rangesMutable = MutableStateFlow(DisplayRanges())
    val ranges = rangesMutable.asStateFlow()
    private val diagnosticControl = MutableStateFlow(DiagnosticControls())
    val diagnosticControls = diagnosticControl.asStateFlow()
    private var visible = false
    fun diagnosticVisibility(value: Boolean) {
        visible = value
        if (!value) diagnosticControl.value = diagnosticControl.value.copy(detailed = false)
        applyDiagnosticControl()
    }
    fun diagnosticPanel(value: Boolean) {
        diagnosticControl.value = DiagnosticControls(open = value, detailed = if (value) diagnosticControl.value.detailed else false)
        applyDiagnosticControl()
    }
    fun diagnosticDetail(value: Boolean) {
        diagnosticControl.value = diagnosticControl.value.copy(detailed = value && diagnosticControl.value.open && visible)
        applyDiagnosticControl()
    }
    private fun applyDiagnosticControl() = app.connections.detailedDiagnostics(visible && diagnosticControl.value.open && diagnosticControl.value.detailed)
    override fun onCleared() { app.connections.detailedDiagnostics(false) }
    init {
        load()
        viewModelScope.launch { combine(connection, diagnostics) { state, sample -> state to sample }.collect { (state, sample) ->
            rangesMutable.update { it.update(state, sample) }
        } }
    }
    private fun load() { viewModelScope.launch {
        try {
            val pair = withContext(Dispatchers.IO) { val settings = app.profiles.read(); settings to if (settings.password == null) "" else app.profiles.decrypt(settings) }
            mutable.update { it.copy(settings = pair.first, password = pair.second, savedSettings = pair.first, savedPassword = pair.second,
                httpPort = pair.first.httpPort.toString(), socksPort = pair.first.socksPort.toString(), loaded = true, error = null) }
        } catch (e: CancellationException) { throw e }
          catch (e: Exception) { mutable.update { it.copy(loaded = false, error = (e as? ClientFailure)?.code ?: "settings_read_failed") } }
    } }
    private fun changeDraft(change: (Editor) -> Editor) {
        if (mutable.value.busy || pending.value != null || permissionRequest != null) return
        generation.incrementAndGet()
        mutable.update {
            val changed = change(it).copy(error = null)
            val checked = validateDraft(changed).errors
            changed.copy(fieldErrors = checked.filterKeys { field -> field in it.fieldErrors })
        }
    }
    fun edit(change: (Settings) -> Settings) = changeDraft { it.copy(settings = change(it.settings)) }
    fun text(field: ProfileField, value: String) {
        if (mutable.value.busy || pending.value != null || permissionRequest != null) return
        changeDraft { editText(it, field, value) }
        mutable.update { editor -> editor.copy(fieldErrors = editor.fieldErrors + editor.rejectedInput +
            if (field == ProfileField.PASSWORD && editor.password.encodeToByteArray().size > 72) mapOf(field to "invalid_password") else emptyMap()) }
    }
    fun save(then: (Mode) -> Unit) { viewModelScope.launch {
        val value = mutable.value
        if (!value.loaded || value.busy || pending.value != null || permissionRequest != null) return@launch
        if (connection.value.phase == "stopping") return@launch
        if (connection.value.alwaysOn && value.settings.mode != Mode.VPN) { error("always_on_mode_required"); return@launch }
        val checked = validateDraft(value)
        if (checked.settings == null) { mutable.update { it.copy(fieldErrors = checked.errors, error = "profile_validation_failed") }; return@launch }
        mutable.update { it.copy(busy = true, error = null) }
        try {
            val saved = withContext(Dispatchers.IO) {
                if (checked.settings.mode == Mode.VPN) checked.settings.packages.forEach { name ->
                    try { app.packageManager.getApplicationInfo(name, 0) }
                    catch (_: android.content.pm.PackageManager.NameNotFoundException) { throw ClientFailure("selected_app_missing") }
                }
                app.profiles.save(checked.settings, value.password)
            }
            mutable.update { it.copy(settings = saved, savedSettings = saved, savedPassword = value.password,
                httpPort = saved.httpPort.toString(), socksPort = saved.socksPort.toString(), fieldErrors = emptyMap()) }
            submittedMutable.emit(Unit); then(saved.mode)
        } catch (e: CancellationException) { throw e }
          catch (e: Exception) { val code = (e as? ClientFailure)?.code ?: "settings_write_failed"
            mutable.update { it.copy(error = code, fieldErrors = if (code == "selected_app_missing") mapOf(ProfileField.APPLICATIONS to code) else it.fieldErrors) } }
        finally { mutable.update { it.copy(busy = false) } }
    } }
    fun importCa(uri: Uri) { viewModelScope.launch {
        if (mutable.value.busy || !mutable.value.loaded) return@launch
        generation.incrementAndGet(); mutable.update { it.copy(busy = true) }
        try {
            withContext(Dispatchers.IO) { app.trust.import(uri); app.profiles.setCustomCa(true) }
            mutable.update { it.copy(settings = it.settings.copy(customCa = true), savedSettings = it.savedSettings.copy(customCa = true), error = null) }
        } catch (e: CancellationException) { throw e }
          catch (e: Exception) { mutable.update { it.copy(error = (e as? ClientFailure)?.code ?: "ca_import_failed") } }
        finally { mutable.update { it.copy(busy = false) } }
    } }
    fun listApps() { viewModelScope.launch {
        if (mutable.value.appsLoading) return@launch
        mutable.update { it.copy(appsLoading = true, appsError = null) }
        try {
            val selected = mutable.value.settings.packages
            val apps = withContext(Dispatchers.IO) {
                val installed = app.packageManager.getInstalledApplications(0)
                if (installed.size > 4096) throw ClientFailure("app_inventory_budget_exceeded")
                val values = installed.filter { it.packageName != app.packageName }.map { SelectableApp(it.packageName, app.packageManager.getApplicationLabel(it).toString().take(256)) }
                (values + (selected - values.map { it.name }.toSet() - app.packageName).map { SelectableApp(it, it, true) }).sortedBy { it.label.lowercase() }
            }
            mutable.update { it.copy(apps = apps) }
        } catch (e: CancellationException) { throw e }
          catch (e: Exception) { mutable.update { it.copy(appsError = (e as? ClientFailure)?.code ?: "app_inventory_failed") } }
        finally { mutable.update { it.copy(appsLoading = false) } }
    } }
    fun selected(name: String, value: Boolean) {
        if (name == app.packageName) return
        if (value && name !in mutable.value.settings.packages && mutable.value.settings.packages.size >= 512) { error("app_selection_budget_exceeded"); return }
        edit { it.copy(packages = if (value) it.packages + name else it.packages - name) }
    }
    fun start(mode: Mode, expectedRevision: Long = revision) {
        if (expectedRevision != revision) return
        val service = if (mode == Mode.VPN) TunnelService::class.java else ProxyService::class.java
        try { app.startForegroundService(Intent(app, service).setAction(ACTION_START)) }
        catch (_: Exception) { app.connections.report("foreground_start_failed") }
    }
    fun stop() { finishStartup(); generation.incrementAndGet(); app.connections.stop() }
    fun refreshPowerState() {
        val exempt = app.getSystemService(android.os.PowerManager::class.java).isIgnoringBatteryOptimizations(app.packageName)
        mutable.update { it.copy(batteryExempt = exempt) }
    }
    fun error(code: String) { mutable.update { it.copy(error = code) } }
}
