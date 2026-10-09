package org.skvoz.android.ui

import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.SolidColor
import androidx.compose.ui.graphics.StrokeCap
import androidx.compose.ui.graphics.StrokeJoin
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.graphics.vector.path
import androidx.compose.ui.unit.dp

internal enum class Glyph { BACK, MENU, NEXT, COPY, SERVER, SHIELD, APPS, SETTINGS, JOURNAL, POWER, ERROR, CHECK, DOWN, UP, CLOSE }
internal val glyphs: Map<Glyph, ImageVector> = Glyph.entries.associateWith { glyph ->
    ImageVector.Builder(glyph.name, 24.dp, 24.dp, 24f, 24f).apply {
        path(stroke = SolidColor(Color.White), strokeLineWidth = 1.6f, strokeLineCap = StrokeCap.Round, strokeLineJoin = StrokeJoin.Round) {
            when (glyph) {
                Glyph.BACK -> { moveTo(14f, 5f); lineTo(7f, 12f); lineTo(14f, 19f); moveTo(7f, 12f); lineTo(21f, 12f) }
                Glyph.MENU -> { for (y in listOf(5f, 12f, 19f)) { moveTo(12f, y); lineTo(12f, y + 0.1f) } }
                Glyph.NEXT -> { moveTo(9f, 5f); lineTo(16f, 12f); lineTo(9f, 19f) }
                Glyph.COPY -> { moveTo(8f, 8f); lineTo(20f, 8f); lineTo(20f, 21f); lineTo(8f, 21f); close(); moveTo(16f, 4f); lineTo(4f, 4f); lineTo(4f, 17f) }
                Glyph.SERVER -> { moveTo(4f, 4f); lineTo(20f, 4f); lineTo(20f, 20f); lineTo(4f, 20f); close(); moveTo(4f, 12f); lineTo(20f, 12f); moveTo(8f, 8f); lineTo(10f, 8f); moveTo(8f, 16f); lineTo(10f, 16f) }
                Glyph.SHIELD -> { moveTo(12f, 3f); lineTo(20f, 6f); lineTo(19f, 15f); lineTo(12f, 21f); lineTo(5f, 15f); lineTo(4f, 6f); close(); moveTo(8f, 12f); lineTo(11f, 15f); lineTo(16f, 9f) }
                Glyph.APPS -> { for (x in listOf(4f, 14f)) for (y in listOf(4f, 14f)) { moveTo(x, y); lineTo(x+6, y); lineTo(x+6, y+6); lineTo(x, y+6); close() } }
                Glyph.SETTINGS -> { moveTo(4f, 6f); lineTo(20f, 6f); moveTo(4f, 12f); lineTo(20f, 12f); moveTo(4f, 18f); lineTo(20f, 18f); moveTo(8f, 3f); lineTo(8f, 9f); moveTo(16f, 9f); lineTo(16f, 15f); moveTo(10f, 15f); lineTo(10f, 21f) }
                Glyph.JOURNAL -> { moveTo(5f, 3f); lineTo(19f, 3f); lineTo(19f, 21f); lineTo(5f, 21f); close(); for (y in listOf(7f, 12f, 17f)) { moveTo(9f, y); lineTo(15f, y) } }
                Glyph.POWER -> { moveTo(12f, 3f); lineTo(12f, 12f); moveTo(7f, 6f); curveTo(0f, 12f, 5f, 21f, 12f, 21f); curveTo(19f, 21f, 24f, 12f, 17f, 6f) }
                Glyph.ERROR -> { moveTo(12f, 5f); lineTo(12f, 13f); moveTo(12f, 18f); lineTo(12f, 18.1f) }
                Glyph.CHECK -> { moveTo(5f, 12f); lineTo(10f, 17f); lineTo(20f, 6f) }
                Glyph.DOWN -> { moveTo(12f, 3f); lineTo(12f, 20f); moveTo(6f, 14f); lineTo(12f, 20f); lineTo(18f, 14f) }
                Glyph.UP -> { moveTo(12f, 21f); lineTo(12f, 4f); moveTo(6f, 10f); lineTo(12f, 4f); lineTo(18f, 10f) }
                Glyph.CLOSE -> { moveTo(5f, 5f); lineTo(19f, 19f); moveTo(19f, 5f); lineTo(5f, 19f) }
            }
        }
    }.build()
}
