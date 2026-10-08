package org.skvoz.android

import android.net.ConnectivityManager
import android.net.LinkProperties
import android.net.Network
import android.net.NetworkCapabilities
import java.io.Closeable

/** All mutable route fields belong exclusively to the connectivity callback thread. */
internal class NetworkMonitor(private val manager: ConnectivityManager, private val signals: RunSignals) : Closeable {
    private var current: Network? = null
    private var capabilities: NetworkCapabilities? = null
    private var properties: LinkProperties? = null
    private var signature: String? = null
    private var registered = false
    private val callback = object : ConnectivityManager.NetworkCallback() {
        override fun onAvailable(network: Network) { current = network; capabilities = null; properties = null }
        override fun onCapabilitiesChanged(network: Network, value: NetworkCapabilities) {
            if (current == network) { capabilities = value; changed() }
        }
        override fun onLinkPropertiesChanged(network: Network, value: LinkProperties) {
            if (current == network) { properties = value; changed() }
        }
        override fun onLost(network: Network) {
            if (current == network) { current = null; capabilities = null; properties = null; signature = null; signals.changed() }
        }
        private fun changed() {
            val cap = capabilities ?: return
            val props = properties ?: return
            if (!cap.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET) || cap.hasTransport(NetworkCapabilities.TRANSPORT_VPN)) return
            val value = "${current}:${props.interfaceName}:${props.linkAddresses.sortedBy { it.toString() }}:${props.routes.sortedBy { it.toString() }}:${props.dnsServers.map { it.hostAddress }.sortedBy { it }}:${props.mtu}:${props.httpProxy}"
            if (value == signature) return
            val first = signature == null; signature = value
            if (!first) signals.changed()
        }
    }
    fun register() { manager.registerDefaultNetworkCallback(callback); registered = true }
    override fun close() { signals.retire(); if (registered) { registered = false; manager.unregisterNetworkCallback(callback) } }
}
