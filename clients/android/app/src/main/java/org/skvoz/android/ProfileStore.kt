package org.skvoz.android

import android.content.Context
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.Base64
import androidx.datastore.core.CorruptionException
import androidx.datastore.core.DataStoreFactory
import androidx.datastore.core.Serializer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.first
import kotlinx.serialization.SerializationException
import kotlinx.serialization.encodeToString
import java.io.InputStream
import java.io.OutputStream
import java.security.KeyStore
import java.security.SecureRandom
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

internal object SettingsSerializer : Serializer<Settings> {
    override val defaultValue = Settings()
    override suspend fun readFrom(input: InputStream): Settings {
        val bytes = input.readBounded(65536)
        if (bytes.size > 65536) throw CorruptionException("Settings budget exceeded")
        return try { wireJson.decodeFromString<Settings>(bytes.decodeToString()).also { it.validate() } }
        catch (e: SerializationException) { throw CorruptionException("Invalid settings", e) }
    }
    override suspend fun writeTo(t: Settings, output: OutputStream) {
        t.validate()
        val bytes = wireJson.encodeToString(t).encodeToByteArray()
        if (bytes.size > 65536) throw ClientFailure("settings_budget_exceeded")
        output.write(bytes)
    }
}
internal class ProfileStore(context: Context, private val keyAlias: String = KEY) {
    private val store = DataStoreFactory.create(serializer = SettingsSerializer,
        scope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
        produceFile = { context.filesDir.resolve("profile.json") })
    private val keyStore by lazy { KeyStore.getInstance("AndroidKeyStore").apply { load(null) } }
    suspend fun read(): Settings = try { store.data.first().also { it.validate() } }
        catch (e: ClientFailure) { throw e }
        catch (e: CancellationException) { throw e }
        catch (_: Exception) { throw ClientFailure("settings_read_failed") }
    suspend fun save(value: Settings, plaintext: String): Settings {
        value.validate()
        if (plaintext.encodeToByteArray().size !in 12..72) throw ClientFailure("invalid_password")
        val existing = read()
        // Existing ciphertext with a lost key is an explicit failure, not new defaults.
        if (existing.password != null && !keyStore.containsAlias(keyAlias)) throw ClientFailure("credential_key_missing")
        val encrypted = encrypt(plaintext, value)
        val device = existing.device.ifEmpty { SecureRandom().let { random -> ByteArray(16).also(random::nextBytes).joinToString("") { "%02x".format(it) } } }
        val saved = value.copy(password = encrypted, device = device)
        saved.validate(true)
        return try { store.updateData { saved } }
        catch (e: CancellationException) { throw e }
        catch (_: Exception) { throw ClientFailure("settings_write_failed") }
    }
    suspend fun setCustomCa(custom: Boolean): Settings = try { store.updateData { it.copy(customCa = custom) } }
        catch (e: CancellationException) { throw e }
        catch (_: Exception) { throw ClientFailure("settings_write_failed") }
    fun decrypt(settings: Settings): String {
        val sealed = settings.password ?: throw ClientFailure("profile_incomplete")
        try {
            val key = keyStore.getKey(keyAlias, null) as? SecretKey ?: throw ClientFailure("credential_key_missing")
            val iv = Base64.decode(sealed.iv, Base64.NO_WRAP)
            val bytes = Base64.decode(sealed.ciphertext, Base64.NO_WRAP)
            if (iv.size != 12 || bytes.size !in 28..88) throw ClientFailure("credential_corrupt")
            val cipher = Cipher.getInstance("AES/GCM/NoPadding")
            cipher.init(Cipher.DECRYPT_MODE, key, GCMParameterSpec(128, iv)); cipher.updateAAD(aad(settings))
            val clear = cipher.doFinal(bytes)
            return try { clear.decodeToString(throwOnInvalidSequence = true) } finally { clear.fill(0) }
        } catch (e: ClientFailure) { throw e }
        catch (_: Exception) { throw ClientFailure("credential_decrypt_failed") }
    }
    private fun encrypt(plaintext: String, settings: Settings): SealedPassword {
        try {
            val key = (keyStore.getKey(keyAlias, null) as? SecretKey) ?: KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, "AndroidKeyStore").apply {
                init(KeyGenParameterSpec.Builder(keyAlias, KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT)
                    .setKeySize(256).setBlockModes(KeyProperties.BLOCK_MODE_GCM).setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                    .setRandomizedEncryptionRequired(true).setUserAuthenticationRequired(false).build())
            }.generateKey()
            val cipher = Cipher.getInstance("AES/GCM/NoPadding"); cipher.init(Cipher.ENCRYPT_MODE, key); cipher.updateAAD(aad(settings))
            val clear = plaintext.encodeToByteArray()
            val bytes = try { cipher.doFinal(clear) } finally { clear.fill(0) }
            if (cipher.iv.size != 12) throw ClientFailure("credential_encrypt_failed")
            return SealedPassword(Base64.encodeToString(cipher.iv, Base64.NO_WRAP), Base64.encodeToString(bytes, Base64.NO_WRAP))
        } catch (_: Exception) { throw ClientFailure("credential_encrypt_failed") }
    }
    private fun aad(settings: Settings) = "SKVOZ:1:${settings.endpoint}:${settings.login}".encodeToByteArray()
    companion object { private const val KEY = "skvoz.profile.aes.v1" }
}
