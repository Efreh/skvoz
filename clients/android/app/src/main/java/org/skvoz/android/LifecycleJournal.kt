package org.skvoz.android

import android.content.Context
import androidx.datastore.core.DataStoreFactory
import androidx.datastore.core.Serializer
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import java.io.InputStream
import java.io.OutputStream

@Serializable internal data class JournalFile(val schema: Int = 1, val entries: List<JournalEntry> = emptyList())
internal object JournalSerializer : Serializer<JournalFile> {
    override val defaultValue = JournalFile()
    override suspend fun readFrom(input: InputStream): JournalFile {
        val value = wireJson.decodeFromString<JournalFile>(input.readBounded(32768).decodeToString())
        if (value.schema != 1 || value.entries.size > 200 || value.entries.any { it.time < 0 || it.text != journalCode(it.text) })
            throw ClientFailure("journal_read_failed")
        return value
    }
    override suspend fun writeTo(t: JournalFile, output: OutputStream) {
        val bytes = wireJson.encodeToString(t).encodeToByteArray()
        if (bytes.size > 32768) throw ClientFailure("journal_write_failed")
        output.write(bytes)
    }
}
/** One bounded snapshot writer; stats do not generate disk writes. */
internal class LifecycleJournal(context: Context) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val store = DataStoreFactory.create(serializer = JournalSerializer, scope = scope,
        produceFile = { context.filesDir.resolve("lifecycle.json") })
    private val lock = Any()
    private val mutable = MutableStateFlow<List<JournalEntry>>(emptyList())
    val entries = mutable.asStateFlow()
    private val changed = Channel<Unit>(Channel.CONFLATED)
    init {
        scope.launch {
            val restored = try { store.data.first().entries }
                catch (e: CancellationException) { throw e }
                catch (_: Exception) { listOf(JournalEntry(System.currentTimeMillis(), "journal_read_failed")) }
            synchronized(lock) { mutable.value = restoreJournal(restored, mutable.value) }
            changed.trySend(Unit)
            for (signal in changed) {
                val snapshot = synchronized(lock) { JournalFile(entries = mutable.value) }
                try { store.updateData { snapshot } }
                catch (e: CancellationException) { throw e }
                catch (_: Exception) {
                    synchronized(lock) { mutable.value = journal(mutable.value, System.currentTimeMillis(), "journal_write_failed") }
                }
            }
        }
        append("process_started")
    }
    fun append(code: String) {
        synchronized(lock) { mutable.value = journal(mutable.value, System.currentTimeMillis(), code) }
        changed.trySend(Unit)
    }
}
