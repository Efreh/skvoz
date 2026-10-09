package org.skvoz.android

import android.app.Application

class SkvozApplication : Application() {
    internal lateinit var profiles: ProfileStore
    internal lateinit var trust: TrustedCa
    internal lateinit var journal: LifecycleJournal
    internal lateinit var connections: ConnectionController
    internal lateinit var displayPreferences: DisplayPreferencesStore
    override fun onCreate() {
        super.onCreate()
        profiles = ProfileStore(this)
        displayPreferences = DisplayPreferencesStore(this)
        trust = TrustedCa(this)
        journal = LifecycleJournal(this)
        connections = ConnectionController(this)
    }
}
