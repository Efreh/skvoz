package org.skvoz.android

import androidx.activity.ComponentActivity
import androidx.compose.foundation.layout.Column
import androidx.compose.runtime.*
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.Density
import androidx.compose.ui.test.*
import androidx.compose.ui.test.junit4.v2.createAndroidComposeRule
import org.junit.Assert.*
import org.junit.Rule
import org.junit.Test
import org.skvoz.android.ui.*

class TelemetryUiDeviceTest {
    @get:Rule val compose = createAndroidComposeRule<ComponentActivity>()

    @Test fun formatRowsSelectOnceAndRetainAccessibleTargets() {
        var clicks = 0
        var toggles = 0
        compose.setContent {
            var preferences by remember { mutableStateOf(DisplayPreferenceState(DisplayPreferences())) }
            SkvozTheme {
                SettingsScreen(Editor(loaded = true), ConnectionState(), {}, {}, {}, {}, {}, preferences,
                    format = { selected -> clicks++; preferences = preferences.copy(value = preferences.value!!.copy(speedFormat = selected)) },
                    resources = { show -> toggles++; preferences = preferences.copy(value = preferences.value!!.copy(showResources = show)) })
            }
        }
        compose.onNodeWithText("Биты/с").performScrollTo().performClick().assertIsSelected()
        compose.runOnIdle { assertEquals(1, clicks) }
        val bounds = compose.onNodeWithText("Биты/с").fetchSemanticsNode().boundsInRoot
        assertTrue(bounds.height >= 48 * compose.activity.resources.displayMetrics.density - 1)
        compose.onNodeWithText("Байты/с").performScrollTo().performClick().assertIsSelected()
        compose.runOnIdle { assertEquals(2, clicks) }
        compose.onNodeWithText("Показывать ресурсы приложения").performScrollTo().assertIsOff().performClick().assertIsOn()
        compose.runOnIdle { assertEquals(1, toggles) }
        val target = compose.onNodeWithText("Показывать ресурсы приложения").fetchSemanticsNode().boundsInRoot
        assertTrue(target.height >= 48 * compose.activity.resources.displayMetrics.density - 1)
        compose.onNodeWithText("Показывать ресурсы приложения").performClick().assertIsOff()
        compose.runOnIdle { assertEquals(2, toggles) }
    }

    @Test fun homeHasOptionalResourceGaugesAtNormalAndLargeFonts() {
        var show by mutableStateOf(false)
        var font by mutableFloatStateOf(1f)
        val sample = ResourceState(cpu = 250.0, pssBytes = 150 * 1048576L, enabled = true)
        compose.setContent {
            val density = LocalDensity.current
            CompositionLocalProvider(LocalDensity provides Density(density.density, font)) {
                SkvozTheme {
                    HomeScreen(Editor(loaded = true), ConnectionState(), null, DisplayRanges(), {}, {}, {}, {}, {}, {}, {}, {},
                        speedFormat = SpeedFormat.BITS, showResources = show, resources = sample, resourceRanges = ResourceRanges().update(sample))
                }
            }
        }
        compose.onNode(hasContentDescription("CPU:", substring = true)).assertDoesNotExist()
        compose.runOnIdle { show = true }
        for (scale in listOf(1f, 2f)) {
            compose.runOnIdle { font = scale }
            compose.onNode(hasContentDescription("CPU:", substring = true) and hasContentDescription("400", substring = true))
                .performScrollTo().assertIsDisplayed()
            compose.onNode(hasContentDescription("RAM (PSS):", substring = true) and hasContentDescription("МиБ", substring = true))
                .performScrollTo().assertIsDisplayed()
        }
        compose.runOnIdle { show = false }
        compose.onNode(hasContentDescription("CPU:", substring = true)).assertDoesNotExist()
    }

    @Test fun meterSemanticsUseBitsWhileMemoryAndTrafficRemainBytes() {
        compose.setContent { SkvozTheme { Column {
            Meter("Получение", 125000uL, 1000000uL, true, speedFormat = SpeedFormat.BITS)
            Meter("Память", 1048576uL, 2097152uL)
            StatLine("Получено", volume(1048576))
        } } }
        compose.onNode(hasContentDescription("Получение:", substring = true) and hasContentDescription("Мбит/с", substring = true)).assertExists()
        compose.onNode(hasContentDescription("Память:", substring = true) and hasContentDescription("МиБ", substring = true)).assertExists()
        compose.onNodeWithText(volume(1048576)).assertExists()
    }
}
