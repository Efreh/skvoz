package org.skvoz.android

import android.content.Context
import android.net.Uri
import android.util.AtomicFile
import java.io.ByteArrayInputStream
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.util.Base64
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509TrustManager

internal class TrustedCa(private val context: Context) {
    private val imported = context.filesDir.resolve("imported-ca.pem")
    private val system = context.filesDir.resolve("system-ca.pem")
    fun path(custom: Boolean): String {
        if (custom) {
            if (!imported.isFile || imported.length() !in 1..262144) throw ClientFailure("trusted_ca_unavailable")
            certificates(imported.readBytes(), 64)
            return imported.absolutePath
        }
        try {
            val factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
            factory.init(null as KeyStore?)
            val anchors = factory.trustManagers.filterIsInstance<X509TrustManager>().flatMap { it.acceptedIssuers.toList() }
            if (anchors.isEmpty() || anchors.size > 512) throw ClientFailure("trusted_ca_unavailable")
            write(system, pem(anchors, 1048576))
            return system.absolutePath
        } catch (e: ClientFailure) { throw e }
        catch (_: Exception) { throw ClientFailure("trusted_ca_export_failed") }
    }
    fun import(uri: Uri) {
        val bytes = try { context.contentResolver.openInputStream(uri)?.use { it.readBounded(262144) } ?: throw ClientFailure("ca_import_failed") }
            catch (_: Exception) { throw ClientFailure("ca_import_failed") }
        if (bytes.isEmpty() || bytes.size > 262144) throw ClientFailure("ca_budget_exceeded")
        write(imported, pem(certificates(bytes, 64), 262144))
    }
    private fun certificates(bytes: ByteArray, max: Int): List<X509Certificate> = try {
        val stream = ByteArrayInputStream(bytes)
        val values = CertificateFactory.getInstance("X.509").generateCertificates(stream).map { it as X509Certificate }
        if (values.isEmpty() || values.size > max || stream.available() != 0 || values.any { it.basicConstraints < 0 }) throw ClientFailure("invalid_ca")
        values.forEach { it.checkValidity() }; values
    } catch (e: ClientFailure) { throw e }
      catch (_: Exception) { throw ClientFailure("invalid_ca") }
    private fun pem(values: List<X509Certificate>, maximum: Int): ByteArray {
        val output = StringBuilder()
        for (certificate in values) {
            val encoded = certificate.encoded
            if (encoded.size > 16384) throw ClientFailure("ca_budget_exceeded")
            output.append("-----BEGIN CERTIFICATE-----\n")
                .append(Base64.getMimeEncoder(64, byteArrayOf(10)).encodeToString(encoded))
                .append("\n-----END CERTIFICATE-----\n")
            if (output.length > maximum) throw ClientFailure("ca_budget_exceeded")
        }
        return output.toString().encodeToByteArray()
    }
    private fun write(file: java.io.File, bytes: ByteArray) {
        val atomic = AtomicFile(file)
        val stream = try { atomic.startWrite() } catch (_: Exception) { throw ClientFailure("ca_write_failed") }
        try { stream.write(bytes); atomic.finishWrite(stream) }
        catch (_: Exception) { atomic.failWrite(stream); throw ClientFailure("ca_write_failed") }
    }
}
