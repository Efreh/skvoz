package org.skvoz.android

import java.io.ByteArrayOutputStream
import java.io.InputStream

/** API31-safe bounded reader; accepts short reads and detects one excess byte. */
internal fun InputStream.readBounded(maximum: Int): ByteArray {
    val output = ByteArrayOutputStream(minOf(maximum, 8192))
    val chunk = ByteArray(8192)
    while (output.size() <= maximum) {
        val count = read(chunk, 0, minOf(chunk.size, maximum + 1 - output.size()))
        if (count < 0) return output.toByteArray()
        if (count == 0) {
            val single = read(); if (single < 0) return output.toByteArray()
            output.write(single)
        } else output.write(chunk, 0, count)
    }
    throw ClientFailure("input_budget_exceeded")
}
