package org.skvoz.android

import android.content.Context
import androidx.datastore.core.CorruptionException
import androidx.datastore.core.DataStoreFactory
import androidx.datastore.core.Serializer
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import java.io.InputStream
import java.io.OutputStream

@Serializable internal enum class SpeedFormat { BYTES, BITS }
@Serializable internal data class DisplayPreferences(val schema: Int = 1, val speedFormat: SpeedFormat = SpeedFormat.BYTES, val showResources: Boolean = false)
internal data class DisplayPreferenceState(val value: DisplayPreferences? = null, val saving: Boolean = false, val error: String? = null) {
    val format get() = value?.speedFormat ?: SpeedFormat.BYTES
    val showResources get() = value?.showResources == true
}

// Presentation preferences never modify the connection profile or its encrypted credentials.
internal object DisplayPreferencesSerializer : Serializer<DisplayPreferences> {
    override val defaultValue = DisplayPreferences()
    override suspend fun readFrom(input: InputStream): DisplayPreferences = try {
        val bytes = input.readBounded(1024)
        if (bytes.size > 1024) throw CorruptionException("Display preferences budget exceeded")
        wireJson.decodeFromString<DisplayPreferences>(bytes.decodeToString()).also {
            if (it.schema != 1) throw CorruptionException("Unsupported display preferences")
        }
    } catch (e: kotlinx.serialization.SerializationException) { throw CorruptionException("Invalid display preferences", e) }
      catch (e: ClientFailure) { throw CorruptionException("Display preferences budget exceeded", e) }
    override suspend fun writeTo(t: DisplayPreferences, output: OutputStream) {
        if (t.schema != 1) throw CorruptionException("Unsupported display preferences")
        output.write(wireJson.encodeToString(t).encodeToByteArray())
    }
}

internal class DisplayPreferencesStore(context: Context) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val store = DataStoreFactory.create(serializer = DisplayPreferencesSerializer, scope = scope,
        produceFile = { context.filesDir.resolve("display.json") })
    private val mutable = MutableStateFlow(DisplayPreferenceState())
    val state = mutable.asStateFlow()
    init { scope.launch {
        try { store.data.collect { value -> mutable.update { it.copy(value = value, error = null) } } }
        catch (e: CancellationException) { throw e }
        catch (_: Exception) { mutable.update { it.copy(error = "display_settings_read_failed") } }
    } }
    suspend fun select(format: SpeedFormat) = change { it.copy(speedFormat = format) }
    suspend fun resources(show: Boolean) = change { it.copy(showResources = show) }
    private suspend fun change(transform: (DisplayPreferences) -> DisplayPreferences) {
        val previous = mutable.value
        val value = previous.value ?: return
        if (previous.saving || transform(value) == value) return
        mutable.update { it.copy(saving = true, error = null) }
        try {
            val saved = store.updateData(transform)
            mutable.update { it.copy(value = saved) }
        } catch (e: CancellationException) { throw e }
          catch (_: Exception) { mutable.update { it.copy(error = "display_settings_write_failed") } }
        finally { mutable.update { it.copy(saving = false) } }
    }
}
