package org.skvoz.android

import android.content.ClipData
import androidx.activity.compose.BackHandler
import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.*
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.navigation.NavDestination.Companion.hasRoute
import androidx.navigation.compose.*
import androidx.window.layout.FoldingFeature
import kotlinx.coroutines.launch
import kotlinx.serialization.Serializable
import org.skvoz.android.ui.*

@Serializable internal data object ConnectionRoute
@Serializable internal data object ProfileRoute
@Serializable internal data object ApplicationsRoute
@Serializable internal data object SettingsRoute
@Serializable internal data object ProxyRoute
@Serializable internal data object TrustRoute
@Serializable internal data object DiagnosticsRoute

// Navigation only stores destinations and small UI state, never profile credentials.
@Composable internal fun ConnectionScreen(
    model: MainViewModel, onConnect: () -> Unit, onImport: () -> Unit,
    onVpnSettings: () -> Unit, onBatterySettings: () -> Unit, onBackground: () -> Unit,
    homeRequest: Long, folds: List<FoldingFeature>,
) {
    val editor by model.editor.collectAsStateWithLifecycle()
    val state by model.connection.collectAsStateWithLifecycle()
    val startup by model.startup.collectAsStateWithLifecycle()
    val diagnostics by model.diagnostics.collectAsStateWithLifecycle()
    val controls by model.diagnosticControls.collectAsStateWithLifecycle()
    val ranges by model.ranges.collectAsStateWithLifecycle()
    val preferences by model.displayPreferences.collectAsStateWithLifecycle()
    val resources by model.resources.collectAsStateWithLifecycle()
    val resourceRanges by model.resourceRanges.collectAsStateWithLifecycle()
    val nav = rememberNavController()
    val entry by nav.currentBackStackEntryAsState()
    val route = entry?.destination
    val home = route == null || route.hasRoute<ConnectionRoute>()
    var menu by remember { mutableStateOf(false) }
    var firstRunPresented by rememberSaveable { mutableStateOf(false) }
    val snack = remember { SnackbarHostState() }
    val scope = rememberCoroutineScope()
    val clipboard = LocalClipboard.current
    fun showHome() { if (editor.loaded) nav.popBackStack<ConnectionRoute>(inclusive = false) }
    fun go(destination: Any) { menu = false; nav.navigate(destination) { launchSingleTop = true } }
    val copy: (String) -> Unit = { text -> scope.launch {
        clipboard.setClipEntry(ClipEntry(ClipData.newPlainText("SKVOZ", text)))
        snack.showSnackbar("Скопировано")
    } }
    LaunchedEffect(editor.loaded) {
        if (editor.loaded && !firstRunPresented) {
            firstRunPresented = true
            if (!editor.hasProfile && !state.active && !state.alwaysOn) go(ProfileRoute)
        }
    }
    LaunchedEffect(homeRequest, editor.loaded) { if (homeRequest > 0 && editor.loaded) showHome() }
    LaunchedEffect(model) { model.submitted.collect { showHome() } }
    LaunchedEffect(editor.fieldErrors, editor.error) {
        if (editor.error == "profile_validation_failed" || editor.error == "selected_app_missing") {
            when (editor.fieldErrors.keys.firstOrNull()) {
                ProfileField.HTTP_PORT, ProfileField.SOCKS_PORT -> if (route?.hasRoute<ProxyRoute>() != true) go(ProxyRoute)
                ProfileField.APPLICATIONS -> if (route?.hasRoute<ApplicationsRoute>() != true) go(ApplicationsRoute)
                null -> Unit
                else -> if (route?.hasRoute<ProfileRoute>() != true) go(ProfileRoute)
            }
        }
    }
    val isDiagnostics = route?.hasRoute<DiagnosticsRoute>() == true
    DisposableEffect(isDiagnostics) {
        model.diagnosticPanel(isDiagnostics)
        onDispose { model.diagnosticPanel(false) }
    }
    DisposableEffect(home, editor.loaded) {
        model.resourceHome(home && editor.loaded)
        onDispose { model.resourceHome(false) }
    }
    BackHandler(enabled = home && !menu, onBack = onBackground)
    val title = when {
        route?.hasRoute<ProfileRoute>() == true -> "Профиль подключения"
        route?.hasRoute<ApplicationsRoute>() == true -> "Приложения ВПН"
        route?.hasRoute<SettingsRoute>() == true -> "Настройки"
        route?.hasRoute<ProxyRoute>() == true -> "Локальные адреса"
        route?.hasRoute<TrustRoute>() == true -> "Доверие TLS"
        isDiagnostics -> "Журнал и диагностика"
        else -> "Соединение SKVOZ"
    }
    val focus = LocalFocusManager.current
    val keyboard = LocalSoftwareKeyboardController.current
    val ime = WindowInsets.ime.getBottom(LocalDensity.current) > 0
    val up: () -> Unit = { if (ime) { focus.clearFocus(); keyboard?.hide() } else nav.popBackStack() }
    SkvozTheme {
        Surface(Modifier.fillMaxSize(), color = SkvozColors.Background) {
            HingeSafeArea(folds) {
                Scaffold(containerColor = SkvozColors.Background, contentWindowInsets = WindowInsets.safeDrawing,
                    snackbarHost = { SnackbarHost(snack) }, topBar = {
                        AppHeader(title, home, editor.loaded, menu, { menu = it }, up, { go(SettingsRoute) }, { go(DiagnosticsRoute) })
                    }) { padding ->
                    Box(Modifier.padding(padding).consumeWindowInsets(padding).imePadding()) {
                        if (!editor.loaded) {
                            FormPage {
                                if (editor.error == null) { CircularProgressIndicator(Modifier.size(24.dp)); Hint("Загрузка профиля…") }
                                else { Notice(errorText(editor.error!!), error = true); Hint("Профиль не сброшен. Восстановите сохранённые настройки через сведения о приложении в Android.") }
                            }
                        } else NavHost(nav, startDestination = ConnectionRoute) {
                            composable<ConnectionRoute> { HomeScreen(editor, state, startup, ranges, onConnect, model::stop, onVpnSettings,
                                { go(ProfileRoute) }, { go(ApplicationsRoute) }, { mode -> model.edit { it.copy(mode = mode) } }, copy, { go(TrustRoute) }, preferences.format, preferences.showResources, resources, resourceRanges) }
                            composable<ProfileRoute> { ProfileScreen(editor, state, startup != null, model::text,
                                { mode -> model.edit { it.copy(mode = mode) } }, onConnect, { go(ApplicationsRoute) }) }
                            composable<ApplicationsRoute> { ApplicationsScreen(editor, state, startup != null, model::selected,
                                { policy -> model.edit { it.copy(appPolicy = policy) } }, model::listApps, { focus.clearFocus(); keyboard?.hide(); nav.popBackStack() }) }
                            composable<SettingsRoute> { SettingsScreen(editor, state, { go(ProxyRoute) }, { go(ApplicationsRoute) }, { go(TrustRoute) }, onVpnSettings, onBatterySettings, preferences, model::speedFormat, model::showResources) }
                            composable<ProxyRoute> { ProxyScreen(editor, state, startup != null, model::text, onConnect, copy) }
                            composable<TrustRoute> { TrustScreen(editor, startup != null, onImport, { model.edit { it.copy(customCa = false) } }) }
                            composable<DiagnosticsRoute> { DiagnosticsScreen(state, diagnostics, controls, ranges, model::diagnosticDetail, copy) }
                        }
                    }
                }
            }
        }
    }
}

// Use one unobstructed pane on separating/occluding folds; ordinary wide windows can use two columns.
@Composable private fun HingeSafeArea(folds: List<FoldingFeature>, content: @Composable () -> Unit) {
    val density = LocalDensity.current
    val fold = folds.firstOrNull { it.isSeparating || it.occlusionType == FoldingFeature.OcclusionType.FULL }
    BoxWithConstraints(Modifier.fillMaxSize()) {
        val width = constraints.maxWidth
        val height = constraints.maxHeight
        val bounds = fold?.bounds
        val modifier = if (bounds == null) Modifier.fillMaxSize() else if (fold.orientation == FoldingFeature.Orientation.VERTICAL) {
            val left = bounds.left.coerceIn(0, width)
            val right = bounds.right.coerceIn(0, width)
            if (left >= width - right) Modifier.width(with(density) { left.toDp() }).fillMaxHeight()
            else Modifier.padding(start = with(density) { right.toDp() }).fillMaxSize()
        } else {
            val top = bounds.top.coerceIn(0, height)
            val bottom = bounds.bottom.coerceIn(0, height)
            if (top >= height - bottom) Modifier.height(with(density) { top.toDp() }).fillMaxWidth()
            else Modifier.padding(top = with(density) { bottom.toDp() }).fillMaxSize()
        }
        Box(modifier) { content() }
    }
}
