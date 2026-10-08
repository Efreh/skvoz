package org.skvoz.android

import android.content.ContextWrapper
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import kotlinx.coroutines.runBlocking
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import java.security.KeyStore
import java.util.UUID

/** Requires a real Android Keystore; JVM tests do not substitute for this gate. */
@RunWith(AndroidJUnit4::class)
class CredentialDeviceTest {
    @Test fun ciphertextRoundtripTamperingAndKeyLoss() = runBlocking {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val directory = context.cacheDir.resolve("credential-test-${UUID.randomUUID()}").apply { mkdirs() }
        val isolated = object : ContextWrapper(context) { override fun getFilesDir() = directory }
        val alias = "skvoz.test.${UUID.randomUUID()}"
        val store = ProfileStore(isolated, alias)
        try {
            val saved = store.save(Settings(endpoint = "example.org:4222", login = "android"), "private-test-password")
            assertEquals("private-test-password", store.decrypt(store.read()))
            assertFalse(directory.resolve("profile.json").readText().contains("private-test-password"))
            assertThrows(ClientFailure::class.java) { store.decrypt(saved.copy(endpoint = "other.example:4222")) }
            KeyStore.getInstance("AndroidKeyStore").apply { load(null); deleteEntry(alias) }
            assertEquals("credential_key_missing", assertThrows(ClientFailure::class.java) { store.decrypt(saved) }.code)
        } finally { KeyStore.getInstance("AndroidKeyStore").apply { load(null); deleteEntry(alias) } }
    }
}
