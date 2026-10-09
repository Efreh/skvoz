plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.plugin.compose")
    id("org.jetbrains.kotlin.plugin.serialization")
}
android {
    namespace = "org.skvoz.android"
    compileSdk = 37
    ndkVersion = "30.0.16248370"
    buildToolsVersion = "37.0.0"
    defaultConfig {
        applicationId = "org.skvoz.android"
        minSdk = 31
        targetSdk = 37
        versionCode = providers.gradleProperty("skvozVersionCode").orElse("6").get().toInt()
        versionName = "1.2.0"
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        ndk { abiFilters += listOf("arm64-v8a", "x86_64") }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    buildFeatures { compose = true; buildConfig = true }
    sourceSets.getByName("main").jniLibs.directories.add(file("build/generated/jniLibs").absolutePath)
    packaging { jniLibs.useLegacyPackaging = false }
    signingConfigs {
        create("official") {
            val path = providers.environmentVariable("SKVOZ_SIGNING_KEYSTORE").orNull
            if (path != null) {
                storeFile = file(path)
                storePassword = providers.environmentVariable("SKVOZ_SIGNING_STORE_PASSWORD").get()
                keyAlias = "skvoz-android"
                keyPassword = storePassword
            }
        }
    }
    buildTypes {
        release { isMinifyEnabled = false; if (providers.environmentVariable("SKVOZ_SIGNING_KEYSTORE").isPresent) signingConfig = signingConfigs.getByName("official") }
    }
}
val buildNative = tasks.register<Exec>("buildNative") {
    workingDir(rootDir)
    commandLine("python3", "tools/build-native.py")
    inputs.files(rootProject.fileTree("native"), rootProject.fileTree("../../network") { exclude("**/target/**", "**/.*/**") }, rootProject.fileTree("../../core") { exclude("**/target/**", "**/.*/**") }, rootProject.fileTree("../../vendor/async-nats") { exclude("**/target/**", "**/.*/**") }, rootProject.file("../../Cargo.toml"), rootProject.file("../../Cargo.lock"), rootProject.file("tools/build-native.py"))
    outputs.dir(layout.buildDirectory.dir("generated/jniLibs"))
}
tasks.named("preBuild") { dependsOn(buildNative) }
dependencies {
    val composeBom = platform("androidx.compose:compose-bom:2026.09.00")
    implementation(composeBom)
    androidTestImplementation(composeBom)
    implementation("androidx.activity:activity-compose:1.13.0")
    implementation("androidx.navigation:navigation-compose:2.10.2")
    implementation("androidx.core:core-ktx:1.19.1")
    implementation("androidx.window:window:1.5.1")
    implementation("androidx.compose.material3:material3")
    implementation("androidx.compose.foundation:foundation")
    implementation("androidx.compose.ui:ui-tooling-preview")
    debugImplementation("androidx.compose.ui:ui-tooling")
    implementation("androidx.lifecycle:lifecycle-runtime-compose:2.11.0")
    implementation("androidx.lifecycle:lifecycle-viewmodel-compose:2.11.0")
    implementation("androidx.datastore:datastore:1.2.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.11.0")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.11.0")
    testImplementation("junit:junit:4.13.2")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.11.0")
    androidTestImplementation("androidx.test:runner:1.7.0")
    androidTestImplementation("androidx.test.ext:junit:1.3.0")
    androidTestImplementation("androidx.compose.ui:ui-test-junit4")
    debugImplementation("androidx.compose.ui:ui-test-manifest")
}
