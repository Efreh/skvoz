package org.skvoz.android

import org.junit.Assert.*
import org.junit.Test

class PresentationTest {
    private val settings = Settings(endpoint = "example.org:4222", login = "client", password = SealedPassword("iv", "cipher"), device = "a".repeat(32))
    private fun draft() = Editor(settings = settings, savedSettings = settings, password = "secret-value-12", savedPassword = "secret-value-12", loaded = true)

    @Test fun emptyInvalidAndDuplicateRawPortsNeverUseThePreviousPort() {
        assertEquals(18080, validateDraft(draft()).settings!!.httpPort)
        listOf("", "0", "65536", "abc", "-1", " 18080", "1.0").forEach { invalid ->
            val editor = draft().copy(httpPort = invalid)
            assertTrue(editor.dirty)
            val result = validateDraft(editor)
            assertNull(result.settings); assertEquals("invalid_port", result.errors[ProfileField.HTTP_PORT])
            assertEquals(18080, editor.settings.httpPort)
        }
        val duplicate = validateDraft(draft().copy(httpPort = "18081"))
        assertNull(duplicate.settings); assertEquals("duplicate_ports", duplicate.errors[ProfileField.SOCKS_PORT])
        assertEquals(4223, validateDraft(draft().copy(httpPort = "4223")).settings!!.httpPort)
    }

    @Test fun validationKeepsInputAndChecksUtf8BytesAndSelectionPolicy() {
        val raw = draft().copy(settings = settings.copy(endpoint = "bad address", login = "__skvoz_server"), password = "я".repeat(37))
        val result = validateDraft(raw)
        assertEquals(listOf(ProfileField.ENDPOINT, ProfileField.LOGIN, ProfileField.PASSWORD), result.errors.keys.toList())
        assertNull(result.settings); assertEquals("я".repeat(37), raw.password)
        assertNotNull(validateDraft(draft().copy(password = "я".repeat(36))).settings)
        val include = draft().copy(settings = settings.copy(mode = Mode.VPN, appPolicy = AppPolicy.INCLUDE))
        assertEquals("empty_app_selection", validateDraft(include).errors[ProfileField.APPLICATIONS])
        val missing = include.copy(settings = include.settings.copy(packages = setOf("gone.app")), apps = listOf(SelectableApp("gone.app", "gone.app", true)))
        assertEquals("selected_app_missing", validateDraft(missing).errors[ProfileField.APPLICATIONS])
        assertNotNull(validateDraft(missing.copy(settings = missing.settings.copy(mode = Mode.PROXY))).settings)
    }

    @Test fun rejectedOversizedPasteBlocksSubmissionUntilThatFieldIsEdited() {
        val original = draft()
        val rejected = editText(original, ProfileField.ENDPOINT, "x".repeat(261))
        assertEquals(original.settings.endpoint, rejected.settings.endpoint)
        assertNull(validateDraft(rejected).settings)
        assertEquals("input_too_long", validateDraft(rejected).errors[ProfileField.ENDPOINT])
        assertNull(validateDraft(editText(rejected, ProfileField.LOGIN, "next_login")).settings)
        val corrected = editText(rejected, ProfileField.ENDPOINT, "next.example:4222")
        assertTrue(corrected.rejectedInput.isEmpty()); assertNotNull(validateDraft(corrected).settings)
        val password = editText(original, ProfileField.PASSWORD, "я".repeat(37))
        assertEquals("я".repeat(37), password.password); assertNull(validateDraft(password).settings)
    }

    @Test fun editorChangesLeaveActiveIdentityAndActualProxyAddressesIntact() {
        val active = ConnectionState(phase = "connected", display = settings.displaySnapshot().copy(httpUri = "http://127.0.0.1:18080"))
        val changed = draft().copy(settings = settings.copy(endpoint = "next.example:4222", mode = Mode.VPN), httpPort = "19080")
        assertFalse(draft().dirty); assertTrue(changed.dirty)
        assertEquals("example.org:4222", active.display!!.endpoint)
        assertEquals(Mode.PROXY, active.display.mode)
        assertEquals("http://127.0.0.1:18080", active.display.httpUri)
        assertTrue(changed.copy(error = "settings_write_failed").dirty)
        val saved = validateDraft(changed).settings!!
        assertFalse(changed.copy(settings = saved, savedSettings = saved).dirty)
    }

    @Test fun actionsSeparateStartupCancellationReconnectAndAndroidOwnership() {
        assertEquals(ConnectionAction.CONNECT, connectionAction(ConnectionState(), false, false))
        assertEquals(ConnectionAction.CANCEL, connectionAction(ConnectionState(), true, false))
        listOf("preparing", "enrolling", "connecting").forEach { assertEquals(ConnectionAction.CANCEL, connectionAction(ConnectionState(phase = it), false, false)) }
        assertEquals(ConnectionAction.DISCONNECT, connectionAction(ConnectionState(phase = "reconnecting"), false, false))
        assertEquals(ConnectionAction.WAIT, connectionAction(ConnectionState(phase = "stopping", alwaysOn = true), false, false))
        assertEquals(ConnectionAction.VPN_SETTINGS, connectionAction(ConnectionState(phase = "error", alwaysOn = true), false, false))
        assertEquals(ConnectionAction.WAIT, connectionAction(ConnectionState(), false, true))
    }

    @Test fun scalesExpandTogetherWithoutShrinkingOnRetryOrClampingValues() {
        val run = ConnectionState(started = 1, upRate = 9 * 1024 * 1024)
        val first = DisplayRanges().update(run, DiagnosticState(basic = mapOf("queue_bytes" to 1024uL, "buffer_bytes" to 5000uL), run = 1))
        assertEquals(16uL * 1024uL * 1024uL, first.rate)
        assertEquals(2048uL, first.queue); assertEquals(8192uL, first.buffer)
        val retry = first.update(run.copy(phase = "reconnecting", upRate = 0), DiagnosticState())
        assertEquals(first.rate, retry.rate); assertEquals(first.queue, retry.queue)
        val freshRun = retry.update(run.copy(started = 2, upRate = 0), DiagnosticState(basic = mapOf("queue_bytes" to 1048576uL), run = 1))
        assertEquals(INITIAL_RATE_RANGE, freshRun.rate); assertEquals(1024uL, freshRun.queue)
        assertEquals(0, litSegments(0uL, first.rate)); assertEquals(1, litSegments(1uL, first.rate))
        assertEquals(40, litSegments(ULong.MAX_VALUE, ULong.MAX_VALUE))
        assertEquals(ULong.MAX_VALUE, expandedRange(1024uL, ULong.MAX_VALUE, true))
    }

    @Test fun notificationRatesStayExpandedAndUpdatesStopWithScreenOff() {
        val state = ConnectionState(phase = "connected", display = settings.displaySnapshot(), downRate = 1234, upRate = 5678)
        val content = notificationContent(state)
        assertEquals(settings.endpoint, content.title); assertEquals("Подключено", content.status)
        assertFalse(content.status.contains("/с")); assertTrue(content.expanded.contains("↓")); assertTrue(content.expanded.contains("↑"))
        val cadence = NotificationCadence()
        assertTrue(cadence.shouldPublish(content, 1000, true, true))
        val next = content.copy(up = 9999)
        assertFalse(cadence.shouldPublish(next, 1999, true, true))
        assertFalse(cadence.shouldPublish(next, 2000, false, true))
        assertFalse(cadence.shouldPublish(next, 2000, true, false))
        assertTrue(cadence.shouldPublish(next, 2000, true, true))
        assertFalse(cadence.shouldPublish(next, 5000, true, true))
        assertTrue(cadence.shouldPublish(next.copy(status = "Переподключение"), 5001, false, false))
        assertEquals(0L, notificationContent(state.copy(phase = "reconnecting")).down)
    }

    @Test fun searchNeverMutatesTheSelectedPackagesAndMatchesBothNames() {
        val apps = listOf(SelectableApp("org.one", "Браузер"), SelectableApp("org.two", "Почта"), SelectableApp("gone.app", "gone.app", true))
        val selected = setOf("org.one", "gone.app")
        assertEquals("org.one", filteredApps(apps, "БРАУЗЕР").single().name)
        assertEquals("org.two", filteredApps(apps, "ORG.TWO").single().name)
        assertTrue(filteredApps(apps, "no result").isEmpty())
        assertEquals(2, selected.size)
        val include = settings.copy(mode = Mode.VPN, appPolicy = AppPolicy.INCLUDE, packages = selected)
        assertEquals(selected, include.copy(appPolicy = AppPolicy.EXCLUDE).packages)
    }
}
