package org.skvoz.android

/** Control only; packets and TCP bytes never enter Kotlin. Call on a worker. */
object NativeBridge {
    init { System.loadLibrary("skvoz_android") }
    external fun cancellationToken(): Long
    external fun cancelEnrollment(token: Long)
    external fun enroll(profile: String, token: Long): String
    external fun liveHandles(): Int
    external fun start(config: String): Long
    external fun request(handle: Long, request: String, borrowedFd: Int = -1)
    external fun poll(handle: Long): String?
    external fun diagnostics(handle: Long, enabled: Boolean): String
    external fun stop(handle: Long)
    external fun tunName(borrowedFd: Int, mtu: Int): String
}
