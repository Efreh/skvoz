package org.skvoz.android

import androidx.compose.ui.test.*
import androidx.compose.ui.test.junit4.v2.createAndroidComposeRule
import org.junit.Rule
import org.junit.Test

class ScreenDeviceTest {
    @get:Rule val compose = createAndroidComposeRule<MainActivity>()
    @Test fun modesAndSettingsRemainAccessible() {
        compose.waitUntil(timeoutMillis = 5000) {
            compose.onAllNodesWithText("Подключить").fetchSemanticsNodes().isNotEmpty()
        }
        if (compose.onAllNodesWithText("Профиль подключения").fetchSemanticsNodes().isNotEmpty()) {
            compose.onNodeWithText("Пароль").assertExists()
            compose.onNodeWithContentDescription("Назад").performClick()
        }
        compose.onNodeWithContentDescription("Меню").performClick()
        compose.onNodeWithText("Настройки", useUnmergedTree = true).performClick()
        compose.onNodeWithText("Доверие TLS").performClick()
        compose.onNodeWithText("Импорт CA").assertExists()
        compose.onNodeWithText("Проверка TLS обязательна").assertExists()
    }
}
