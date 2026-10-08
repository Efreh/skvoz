package org.skvoz.android;

import java.nio.file.Files;
import java.nio.file.Path;
import java.io.BufferedReader;
import java.io.InputStreamReader;

/** A desktop JVM exercises the identical JNI entrypoints, not Android VPN APIs. */
public final class NativeBridge {
    static { System.loadLibrary("skvoz_android"); }
    public native long cancellationToken();
    public native void cancelEnrollment(long token);
    public native String enroll(String profile, long token);
    public native int liveHandles();
    public native long start(String configuration);
    public native void request(long handle, String request, int borrowedFd);
    public native String poll(long handle);
    public native String diagnostics(long handle, boolean enabled);
    public native void stop(long handle);
    public native String tunName(int fd, int mtu);
    private boolean runtimeReady = false;
    private void response(long handle, int id) {
        long deadline = System.nanoTime() + 20_000_000_000L;
        while (System.nanoTime() < deadline) {
            String result = poll(handle);
            if (result != null && result.contains("\"state\":\"ready\"")) runtimeReady = true;
            if (result != null && result.contains("\"id\":" + id + ",")) {
                if (!result.contains("\"error\":null")) throw new AssertionError("Native API rejected request");
                return;
            }
        }
        throw new AssertionError("Native API response deadline exceeded");
    }
    public static void main(String[] args) throws Exception {
        NativeBridge bridge = new NativeBridge();
        String profile = Files.readString(Path.of(args[0]));
        if (args[1].equals("closure")) {
            String config = bridge.enroll(profile, bridge.cancellationToken());
            for (int cycle = 0; cycle < 2; cycle++) {
                bridge.runtimeReady = false;
                long handle = bridge.start(config);
                try {
                    bridge.request(handle, "{\"v\":1,\"id\":1,\"op\":\"HELLO\",\"args\":{\"api\":1,\"network\":4},\"fd_count\":0}", -1);
                    bridge.response(handle, 1);
                    long deadline = System.nanoTime() + 10_000_000_000L;
                    while (!bridge.runtimeReady && System.nanoTime() < deadline) {
                        String event = bridge.poll(handle);
                        if (event != null && event.contains("\"state\":\"ready\"")) bridge.runtimeReady = true;
                    }
                    if (!bridge.runtimeReady) throw new AssertionError("Replacement failed to become ready");
                    String off = bridge.diagnostics(handle, false);
                    if (!off.contains("\"enabled\":false") || !off.contains("\"sample_age_ms\":null")) throw new AssertionError("Diagnostics default is not off");
                    String pending = bridge.diagnostics(handle, true);
                    if (!pending.contains("\"sample_age_ms\":null")) throw new AssertionError("Old collection published as current");
                    String sample = pending;
                    deadline = System.nanoTime() + 5_000_000_000L;
                    while (sample.contains("\"sample_age_ms\":null") && System.nanoTime() < deadline) {
                        bridge.poll(handle);
                        sample = bridge.diagnostics(handle, true);
                    }
                    if (sample.length() > 4096 || sample.contains("\"sample_age_ms\":null") || !sample.contains("\"enabled\":true")) throw new AssertionError("Missing bounded ready diagnostic sample");
                    bridge.diagnostics(handle, false);
                    String fresh = bridge.diagnostics(handle, true);
                    if (!fresh.contains("\"collection\":3") || !fresh.contains("\"turns\":0") || !fresh.contains("\"sample_age_ms\":null")) throw new AssertionError("Off/on failed to reset collection");
                    bridge.diagnostics(handle, false);
                    bridge.request(handle, "{\"v\":1,\"id\":2,\"op\":\"PREPARE_SHUTDOWN\",\"args\":{},\"fd_count\":0}", -1);
                    bridge.response(handle, 2);
                    boolean closed = false;
                    deadline = System.nanoTime() + 10_000_000_000L;
                    while (!closed && System.nanoTime() < deadline) {
                        try { bridge.poll(handle); }
                        catch (RuntimeException expected) {
                            if (!"Rust error: runtime_lost".equals(expected.getMessage())) throw expected;
                            closed = true;
                        }
                    }
                    if (!closed) throw new AssertionError("Closed poll did not report runtime_lost");
                    try { bridge.diagnostics(handle, true); throw new AssertionError("Closed diagnostics shown live"); }
                    catch (RuntimeException expected) { if (!"Rust error: diagnostics_unavailable".equals(expected.getMessage())) throw expected; }
                    try {
                        bridge.request(handle, "{\"v\":1,\"id\":3,\"op\":\"STATUS\",\"args\":{},\"fd_count\":0}", -1);
                        throw new AssertionError("Closed request accepted");
                    } catch (RuntimeException expected) {
                        if (!"Rust error: runtime_lost".equals(expected.getMessage())) throw expected;
                    }
                } finally { bridge.stop(handle); }
                if (bridge.liveHandles() != 0) throw new AssertionError("Closed runtime owner leaked");
                try { bridge.diagnostics(handle, true); throw new AssertionError("Stale diagnostics accepted"); }
                catch (RuntimeException expected) { if (!"Rust error: stale_native_handle".equals(expected.getMessage())) throw expected; }
            }
            System.out.println("CLOSED_RECOVERABLE_REPLACED_HANDLES0_DIAGNOSTICS_RESET_BOUNDED");
            return;
        }
        if (args[1].equals("cycles")) {
            String config = bridge.enroll(profile, bridge.cancellationToken());
            long baseline;
            try (var files = Files.list(Path.of("/proc/self/fd"))) { baseline = files.count(); }
            for (int cycle = 0; cycle < 50; cycle++) {
                bridge.runtimeReady = false;
                long handle = bridge.start(config);
                if (bridge.liveHandles() != 1) throw new AssertionError("Missing runtime owner");
                bridge.request(handle, "{\"v\":1,\"id\":1,\"op\":\"HELLO\",\"args\":{\"api\":1,\"network\":4},\"fd_count\":0}", -1);
                bridge.response(handle, 1);
                long readyDeadline = System.nanoTime() + 5_000_000_000L;
                while (!bridge.runtimeReady && System.nanoTime() < readyDeadline) {
                    String event = bridge.poll(handle);
                    if (event != null && event.contains("\"state\":\"ready\"")) bridge.runtimeReady = true;
                }
                if (!bridge.runtimeReady) throw new AssertionError("Cycle runtime did not connect");
                bridge.stop(handle);
                if (bridge.liveHandles() != 0) throw new AssertionError("Runtime owner leaked");
            }
            long after;
            try (var files = Files.list(Path.of("/proc/self/fd"))) { after = files.count(); }
            if (after > baseline + 3) throw new AssertionError("Native descriptor growth");
            System.out.println("CYCLES50_HANDLES0_FD=" + baseline + ":" + after);
            return;
        }
        if (args[1].equals("cancel")) {
            long token = bridge.cancellationToken();
            java.util.concurrent.atomic.AtomicReference<Throwable> failure = new java.util.concurrent.atomic.AtomicReference<>();
            Thread cancel = new Thread(() -> {
                try {
                    Thread.sleep(500);
                    try { bridge.enroll(profile, token); throw new AssertionError("Concurrent enrollment accepted"); }
                    catch (RuntimeException expected) {
                        if (!expected.getMessage().endsWith("enrollment_already_active")) throw expected;
                    }
                    bridge.cancelEnrollment(token);
                } catch (Throwable error) { failure.set(error); bridge.cancelEnrollment(token); }
            });
            cancel.start();
            try { bridge.enroll(profile, token); throw new AssertionError("Pending enrollment survived cancellation"); }
            catch (RuntimeException expected) {
                if (!expected.getMessage().endsWith("enrollment_cancelled")) throw expected;
            }
            cancel.join();
            if (failure.get() != null) throw new AssertionError(failure.get());
            long fresh = bridge.cancellationToken();
            bridge.cancelEnrollment(token);
            bridge.enroll(Files.readString(Path.of(args[2])), fresh);
            System.out.println("CANCELLED_RECONNECTED");
            return;
        }
        if (args[1].equals("negative")) {
            try { bridge.enroll(profile, bridge.cancellationToken()); throw new AssertionError("Invalid enrollment accepted"); }
            catch (RuntimeException error) { System.out.println(error.getMessage()); }
            return;
        }
        long staleToken = bridge.cancellationToken();
        bridge.cancelEnrollment(staleToken);
        try { bridge.enroll(profile, staleToken); throw new AssertionError("Cancelled enrollment accepted"); }
        catch (RuntimeException expected) {
            if (!expected.getMessage().endsWith("enrollment_cancelled")) throw expected;
        }
        String config = bridge.enroll(profile, bridge.cancellationToken());
        long handle = bridge.start(config);
        try {
            try { bridge.start(config); throw new AssertionError("Two runtimes accepted"); }
            catch (RuntimeException expected) { }
            bridge.request(handle, "{\"v\":1,\"id\":1,\"op\":\"HELLO\",\"args\":{\"api\":1,\"network\":4},\"fd_count\":0}", -1);
            bridge.response(handle, 1);
            long deadline = System.nanoTime() + 20_000_000_000L;
            boolean ready = bridge.runtimeReady;
            while (!ready && System.nanoTime() < deadline) {
                String event = bridge.poll(handle);
                if (event != null && event.contains("\"state\":\"ready\"")) { ready = true; break; }
            }
            if (!ready) throw new AssertionError("Native runtime readiness deadline exceeded");
            bridge.request(handle, "{\"v\":1,\"id\":2,\"op\":\"START_PROXY\",\"args\":{\"http_bind\":\"127.0.0.1:" + args[1] + "\",\"socks_bind\":\"127.0.0.1:" + args[2] + "\"},\"fd_count\":0}", -1);
            bridge.response(handle, 2);
            try { bridge.tunName(-1, 1500); throw new AssertionError("Invalid descriptor accepted"); }
            catch (RuntimeException expected) { }
            System.out.println("READY"); System.out.flush();
            BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
            String command;
            while ((command = input.readLine()) != null && !command.equals("STOP")) bridge.poll(handle);
        } finally { bridge.stop(handle); }
        try { bridge.poll(handle); throw new AssertionError("Stale handle accepted"); }
        catch (RuntimeException expected) { }
        System.out.println("STOPPED");
    }
}
