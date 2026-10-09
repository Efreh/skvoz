package org.skvoz.android

import android.graphics.Bitmap
import androidx.activity.ComponentActivity
import androidx.activity.SystemBarStyle
import androidx.activity.enableEdgeToEdge
import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.asAndroidBitmap
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.semantics.SemanticsActions
import androidx.compose.ui.test.*
import androidx.compose.ui.test.junit4.v2.createAndroidComposeRule
import androidx.compose.ui.unit.Density
import androidx.compose.ui.unit.DpSize
import androidx.compose.ui.unit.dp
import androidx.test.platform.app.InstrumentationRegistry
import org.junit.Assert.*
import org.junit.Rule
import org.junit.Test
import org.skvoz.android.ui.*
import java.io.File

// Exercise production composables without network requests or persisted profile changes.
@OptIn(ExperimentalTestApi::class)
class UiReferenceDeviceTest {
    @get:Rule val compose = createAndroidComposeRule<ComponentActivity>()
    private val settings = Settings(endpoint = "server.example:4222", login = "user", mode = Mode.VPN)
    private val saved = Editor(settings = settings, savedSettings = settings, loaded = true,
        password = "example-password", savedPassword = "example-password",
        apps = listOf(SelectableApp("org.example.browser", "Браузер"), SelectableApp("org.example.messages", "Сообщения")))
    private data class Scene(val id: String, val route: String = "home", val editor: Editor,
        val state: ConnectionState = ConnectionState(), val diagnostics: DiagnosticState = DiagnosticState(),
        val controls: DiagnosticControls = DiagnosticControls(), val fontScale: Float = 1f,
        val window: DpSize? = null)
    private val scene = mutableStateOf(Scene("initial", editor = saved))
    private var actionCount = 0

    private fun host() {
        compose.activityRule.scenario.onActivity {
            it.enableEdgeToEdge(SystemBarStyle.dark(0xFF181A1D.toInt()), SystemBarStyle.dark(0xFF181A1D.toInt()))
        }
        compose.setContent {
            val current = scene.value
            if (current.window == null) Content(current)
            else DeviceConfigurationOverride(DeviceConfigurationOverride.WindowSize(current.window)) { Content(current) }
        }
    }

    @Composable private fun Content(current: Scene) {
            val actual = LocalDensity.current
            CompositionLocalProvider(LocalDensity provides Density(actual.density, current.fontScale)) {
                SkvozTheme {
                    val title = when (current.route) {
                        "profile" -> "Профиль подключения"; "apps" -> "Приложения ВПН"; "settings" -> "Настройки"
                        "proxy" -> "Локальные адреса"; "trust" -> "Доверие TLS"; "diagnostics" -> "Журнал и диагностика"
                        else -> "Соединение SKVOZ"
                    }
                    Scaffold(containerColor = SkvozColors.Background, contentWindowInsets = WindowInsets.safeDrawing,
                        topBar = { AppHeader(title, current.route == "home", true, false, {}, {}, {}, {}) }) { padding ->
                        Box(Modifier.padding(padding).consumeWindowInsets(padding).imePadding()) {
                            key(current.id) {
                                val action = { actionCount++; Unit }
                                when (current.route) {
                                    "profile" -> ProfileScreen(current.editor, current.state, false, { _, _ -> }, {}, action, {})
                                    "apps" -> ApplicationsScreen(current.editor, current.state, false, { name, checked ->
                                        val editor = scene.value.editor
                                        val packages = if (checked) editor.settings.packages + name else editor.settings.packages - name
                                        scene.value = scene.value.copy(editor = editor.copy(settings = editor.settings.copy(packages = packages)))
                                    }, {}, {}, action)
                                    "settings" -> SettingsScreen(current.editor, current.state, action, action, action, action, action)
                                    "proxy" -> ProxyScreen(current.editor, current.state, false, { _, _ -> }, action, {})
                                    "trust" -> TrustScreen(current.editor, false, action, action)
                                    "diagnostics" -> DiagnosticsScreen(current.state, current.diagnostics, current.controls, DisplayRanges(), {}, {})
                                    else -> HomeScreen(current.editor, current.state, null, DisplayRanges(), action, action, action, {}, {}, {}, {}, {})
                                }
                            }
                        }
                    }
                }
            }
    }

    private fun show(value: Scene) {
        compose.runOnIdle { scene.value = value; actionCount = 0 }
        compose.waitForIdle()
        capture(value.id)
    }

    private fun capture(name: String) {
        if (InstrumentationRegistry.getArguments().getString("captureUiReferences") != "true") return
        val directory = File(compose.activity.cacheDir, "ui-reference").apply { mkdirs() }
        File(directory, "$name.png").outputStream().use {
            compose.onRoot().captureToImage().asAndroidBitmap().compress(Bitmap.CompressFormat.PNG, 100, it)
        }
        File(directory, "$name.txt").writeText(compose.onRoot().printToString())
    }

    private fun reachableAction(label: String, enabled: Boolean = true): SemanticsNodeInteraction {
        val node = compose.onNodeWithText(label)
        val scrollable = generateSequence(node.fetchSemanticsNode().parent) { it.parent }
            .any { it.config.contains(SemanticsActions.ScrollBy) }
        if (scrollable) node.performScrollTo()
        node.assertIsDisplayed()
        if (enabled) node.assertIsEnabled() else node.assertIsNotEnabled()
        val bounds = node.fetchSemanticsNode().boundsInRoot
        val density = compose.activity.resources.displayMetrics.density
        assertTrue("${scene.value.id}: $label height=$bounds", bounds.height + 1 >= 48 * density)
        assertTrue("${scene.value.id}: $label width=$bounds", bounds.width + 1 >= 48 * density)
        return node
    }

    @Test fun everyConnectionPhaseKeepsItsActionAtNormalAndLargeText() {
        host()
        val phases = listOf("disconnected", "preparing", "enrolling", "connecting", "connected", "reconnecting", "stopping", "error")
        for (scale in listOf(1f, 2f)) for (phase in phases) {
            val state = ConnectionState(phase = phase, mode = Mode.VPN, display = settings.displaySnapshot(),
                error = if (phase == "error") "authentication_failed" else null)
            val id = "home-$phase-${scale.toInt()}"
            show(Scene(id, editor = saved, state = state, fontScale = scale))
            val label = when (phase) {
                "preparing", "enrolling", "connecting" -> "Отменить"
                "connected", "reconnecting" -> "Отключить"
                "stopping" -> "Отключение…"
                else -> "Подключить"
            }
            val node = reachableAction(label, phase != "stopping")
            capture("$id-action")
            if (phase != "stopping") { node.performClick(); compose.runOnIdle { assertEquals(1, actionCount) } }
        }
        for (scale in listOf(1f, 2f)) {
            val state = ConnectionState(phase = "connected", mode = Mode.VPN, alwaysOn = true, lockdown = true,
                display = settings.displaySnapshot())
            show(Scene("home-always-on-${scale.toInt()}", editor = saved, state = state, fontScale = scale))
            reachableAction("Настройки ВПН").performClick()
            compose.runOnIdle { assertEquals(1, actionCount) }
        }
    }

    @Test fun formAndDiagnosticsActionsRemainReachableAtNormalAndLargeText() {
        host()
        val states = listOf(
            Scene("profile-saved", "profile", saved),
            Scene("profile-error", "profile", saved.copy(settings = settings.copy(endpoint = "server.example"),
                fieldErrors = mapOf(ProfileField.ENDPOINT to "invalid_address"))),
            Scene("profile-draft", "profile", saved.copy(settings = settings.copy(endpoint = "next.example:4222")),
                ConnectionState(phase = "connected", mode = Mode.VPN, display = settings.displaySnapshot())),
            Scene("apps-include-empty", "apps", saved.copy(settings = settings.copy(appPolicy = AppPolicy.INCLUDE))),
            Scene("apps-missing", "apps", saved.copy(settings = settings.copy(packages = setOf("org.example.missing")),
                apps = listOf(SelectableApp("org.example.missing", "org.example.missing", missing = true)))),
            Scene("settings", "settings", saved), Scene("proxy", "proxy", saved),
            Scene("trust-android", "trust", saved), Scene("trust-imported", "trust", saved.copy(
                settings = settings.copy(customCa = true), savedSettings = settings.copy(customCa = true))),
            Scene("diagnostics-off", "diagnostics", saved),
            Scene("diagnostics-waiting", "diagnostics", saved, controls = DiagnosticControls(true, true)),
            Scene("diagnostics-fresh", "diagnostics", saved, diagnostics = DiagnosticState(
                detail = DiagnosticSnapshot(true, 240uL, DETAILED_METRICS.associateWith { 1uL })), controls = DiagnosticControls(true, true)),
            Scene("diagnostics-stale", "diagnostics", saved, diagnostics = DiagnosticState(
                detail = DiagnosticSnapshot(true, 3400uL, DETAILED_METRICS.associateWith { 1uL })), controls = DiagnosticControls(true, true)),
            Scene("diagnostics-unavailable", "diagnostics", saved, diagnostics = DiagnosticState(code = "diagnostics_unavailable"),
                controls = DiagnosticControls(true, true)),
        )
        for (scale in listOf(1f, 2f)) for (value in states) {
            val current = value.copy(id = "${value.id}-${scale.toInt()}", fontScale = scale)
            show(current)
            val label = when (value.route) {
                "profile", "proxy" -> applyLabel(value.state); "apps" -> "Готово"
                "settings" -> "Доверие TLS"; "trust" -> "Импорт CA"; else -> "Копировать отчёт"
            }
            reachableAction(label)
            capture("${current.id}-action")
        }
    }

    @Test fun appRowHasOneToggleAndKeepsTheFullPackageAtLargeText() {
        host()
        for (scale in listOf(1f, 2f)) {
            show(Scene("apps-toggle-${scale.toInt()}", "apps", saved, fontScale = scale))
            val row = compose.onNodeWithText("Браузер").performScrollTo().assertIsOff()
            row.performClick().assertIsOn()
            compose.runOnIdle { assertEquals(setOf("org.example.browser"), scene.value.editor.settings.packages) }
            compose.onNodeWithText("org.example.browser", useUnmergedTree = true).assertExists()
            capture("apps-selected-${scale.toInt()}")
            row.performClick().assertIsOff()
            compose.runOnIdle { assertTrue(scene.value.editor.settings.packages.isEmpty()) }
        }
    }

    @Test fun wideWindowAddsJournalAndLargeTextOrLowHeightReturnToOneColumn() {
        host()
        val state = ConnectionState(phase = "connected", mode = Mode.VPN, display = settings.displaySnapshot(),
            journal = listOf(JournalEntry(1_791_503_000_000L, "connected")))
        show(Scene("home-wide-900", editor = saved, state = state, window = DpSize(900.dp, 900.dp)))
        val server = compose.onNodeWithText(settings.endpoint).assertIsDisplayed().fetchSemanticsNode().boundsInRoot
        val journal = compose.onNodeWithText("Журнал соединения").assertIsDisplayed().fetchSemanticsNode().boundsInRoot
        assertTrue("Journal must occupy a separate column", journal.left > server.right)
        compose.onNodeWithText("Отключить").assertIsDisplayed()
        for (value in listOf(
            Scene("home-wide-large-text", editor = saved, state = state, fontScale = 2f, window = DpSize(900.dp, 900.dp)),
            Scene("home-short-900", editor = saved, state = state, window = DpSize(900.dp, 400.dp)),
            Scene("home-medium-700", editor = saved, state = state, window = DpSize(700.dp, 900.dp)),
        )) {
            show(value)
            compose.onNodeWithText("Журнал соединения").assertDoesNotExist()
            compose.onNodeWithText("Отключить").performScrollTo().assertIsDisplayed()
            capture("${value.id}-action")
        }
    }
}
