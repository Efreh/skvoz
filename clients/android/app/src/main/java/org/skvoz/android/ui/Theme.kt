package org.skvoz.android.ui

import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.*
import androidx.compose.runtime.Composable
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.TextStyle
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp

internal object SkvozColors {
    val Background = Color(0xFF181A1D)
    val Panel = Color(0xFF23262A)
    val Text = Color(0xFFE4E8E5)
    val Secondary = Color(0xFF9EA8A2)
    val Accent = Color(0xFF76D99C)
    val Light = Color(0xFFA1E680)
    val Warning = Color(0xFFE4B779)
    val Error = Color(0xFFEA9696)
    val Line = Color(0xFF394348)
    val Border = Color(0xFF7C8B82)
    val OnAccent = Color(0xFF132019)
}

internal object SkvozLayout {
    val PagePadding = 16.dp
    val SectionGap = 12.dp
    val PanelPadding = 12.dp
    val ContentGap = 8.dp
    val DetailGap = 4.dp
    val TouchHeight = 48.dp
    val HeaderHeight = 56.dp
    val SummaryHeight = 64.dp
    val ApplicationHeight = 56.dp
    val IconSize = 20.dp
    val MeterHeight = 16.dp
}

@Composable internal fun SkvozTheme(content: @Composable () -> Unit) {
    val colors = darkColorScheme(
        primary = SkvozColors.Accent, onPrimary = SkvozColors.OnAccent,
        primaryContainer = Color(0xFF284535), onPrimaryContainer = SkvozColors.Text,
        secondary = SkvozColors.Light, onSecondary = SkvozColors.OnAccent,
        background = SkvozColors.Background, onBackground = SkvozColors.Text,
        surface = SkvozColors.Panel, onSurface = SkvozColors.Text,
        surfaceVariant = SkvozColors.Panel, onSurfaceVariant = SkvozColors.Secondary,
        surfaceContainer = SkvozColors.Panel, surfaceContainerHigh = SkvozColors.Panel,
        outline = SkvozColors.Border, outlineVariant = SkvozColors.Line,
        error = SkvozColors.Error, onError = SkvozColors.Background,
    )
    fun style(size: Int, line: Int, weight: FontWeight = FontWeight.Normal) = TextStyle(
        fontFamily = FontFamily.SansSerif, fontSize = size.sp, lineHeight = line.sp,
        fontWeight = weight, fontFeatureSettings = "tnum",
    )
    MaterialTheme(colorScheme = colors, shapes = Shapes(
        extraSmall = RoundedCornerShape(6.dp), small = RoundedCornerShape(6.dp),
        medium = RoundedCornerShape(8.dp), large = RoundedCornerShape(8.dp), extraLarge = RoundedCornerShape(8.dp),
    ), typography = Typography(
        headlineSmall = style(20, 26, FontWeight.Medium), titleLarge = style(18, 24, FontWeight.Medium),
        titleMedium = style(16, 22, FontWeight.Medium), titleSmall = style(14, 20, FontWeight.Medium),
        bodyLarge = style(14, 20), bodyMedium = style(13, 18), bodySmall = style(12, 16),
        labelLarge = style(14, 20, FontWeight.Medium), labelMedium = style(13, 18, FontWeight.Medium), labelSmall = style(12, 16),
    ), content = content)
}
