package org.skvoz.android.ui

import android.content.pm.PackageManager
import android.graphics.Bitmap
import android.graphics.Canvas
import android.util.LruCache
import androidx.core.graphics.createBitmap
import androidx.compose.foundation.Image
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.size
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.asImageBitmap
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/** Screen-owned cache; only composed rows request icons, with one IO loader. */
internal class AppIconLoader(private val packages: PackageManager) {
    private val io = Dispatchers.IO.limitedParallelism(1)
    private val cache = object : LruCache<String, Bitmap>(1024 * 1024) {
        override fun sizeOf(key: String, value: Bitmap) = value.byteCount
    }
    suspend fun load(name: String): Bitmap? = withContext(io) {
        cache.get(name) ?: try {
            val drawable = packages.getApplicationIcon(name)
            createBitmap(96, 96).also { bitmap ->
                drawable.setBounds(0, 0, 96, 96)
                drawable.draw(Canvas(bitmap))
                cache.put(name, bitmap)
            }
        } catch (_: PackageManager.NameNotFoundException) { null }
          catch (_: SecurityException) { null }
    }
}

@Composable internal fun rememberAppIconLoader(): AppIconLoader {
    val context = LocalContext.current.applicationContext
    return remember(context) { AppIconLoader(context.packageManager) }
}

@Composable internal fun AppIcon(name: String, missing: Boolean, loader: AppIconLoader) {
    val bitmap by produceState<Bitmap?>(null, name, missing, loader) {
        if (!missing) value = loader.load(name)
    }
    Box(Modifier.size(28.dp), contentAlignment = Alignment.Center) {
        bitmap?.let { Image(it.asImageBitmap(), null, Modifier.size(28.dp)) } ?: Symbol(Glyph.APPS)
    }
}
