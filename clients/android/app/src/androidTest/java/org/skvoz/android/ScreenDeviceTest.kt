package org.skvoz.android

import androidx.compose.ui.test.*
import androidx.compose.ui.test.junit4.v2.createAndroidComposeRule
import org.junit.Rule
import org.junit.Test

class ScreenDeviceTest {
    @get:Rule val compose = createAndroidComposeRule<MainActivity>()
    @Test fun modesAndSettingsRemainAccessible() {
        compose.onNodeWithText("Соединение SKVOZ").assertIsDisplayed()
        compose.onNodeWithText("Настройки").performClick()
        compose.onNodeWithText("Настройки соединения").assertExists()
        compose.onNodeWithText("Импорт CA").assertExists()
        compose.onNodeWithText("Пароль").assertExists()
    }
}
