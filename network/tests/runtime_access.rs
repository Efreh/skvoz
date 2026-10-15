#![cfg(all(target_os = "linux", feature = "portable-runtime"))]
//! Own-loopback qualification of current local admission and native TCP ownership.
use serde_json::{Value, json};
use skvoz_network::{
    PollError, RuntimeHandle,
    config::{Limits, Role, StartupConfig},
};
use std::{
    io::{self, Read, Write},
    net::TcpListener,
    os::unix::net::UnixStream,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

struct Owner {
    runtime: RuntimeHandle,
    id: u32,
}
impl Owner {
    fn new(config: StartupConfig) -> Self {
        let mut owner = Self {
            runtime: RuntimeHandle::start(config, None).unwrap(),
            id: 0,
        };
        assert_eq!(
            owner.call("HELLO", json!({"api":1,"network":6})).0["error"],
            Value::Null
        );
        let until = Instant::now() + Duration::from_secs(15);
        loop {
            if owner.call("STATUS", json!({})).0["result"]["lifecycle"] == "ready" {
                break;
            }
            assert!(
                Instant::now() < until,
                "current runtime did not become ready"
            );
            thread::sleep(Duration::from_millis(20));
        }
        owner
    }
    fn call(&mut self, op: &str, args: Value) -> (Value, Option<std::os::fd::OwnedFd>) {
        self.call_budget(op, args, Duration::from_secs(15))
    }
    fn call_budget(
        &mut self,
        op: &str,
        args: Value,
        budget: Duration,
    ) -> (Value, Option<std::os::fd::OwnedFd>) {
        self.id += 1;
        self.runtime
            .request_json(
                &serde_json::to_vec(&json!({"v":1,"id":self.id,"op":op,"args":args,"fd_count":0}))
                    .unwrap(),
                None,
            )
            .unwrap();
        let until = Instant::now() + budget;
        loop {
            match self.runtime.next_message(32768, Duration::from_millis(100)) {
                Ok(message) => {
                    let value: Value = serde_json::from_slice(&message.json).unwrap();
                    if value["id"] == self.id {
                        return (value, message.fd);
                    }
                }
                Err(PollError::Timeout) => {}
                other => panic!("current runtime response missing: {other:?}"),
            }
            assert!(Instant::now() < until, "current runtime response deadline");
        }
    }
    fn open(&mut self, port: u16) -> UnixStream {
        let until = Instant::now() + Duration::from_secs(12);
        loop {
            let (reply, fd) = self.call("OPEN_TCP", json!({"host":"127.0.0.1","port":port}));
            if reply["error"].is_null() {
                let stream = UnixStream::from(fd.expect("native descriptor missing"));
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                return stream;
            }
            assert_eq!(reply["error"], "network_unavailable");
            assert!(Instant::now() < until, "direct peer did not join");
            thread::sleep(Duration::from_millis(50));
        }
    }
    fn tcp_count(&mut self) -> u64 {
        self.call("STATUS", json!({})).0["result"]["counters"]["tcp_open"]
            .as_u64()
            .unwrap()
    }
}
fn configured(id: u64, port: u16) -> StartupConfig {
    let mut value: Value =
        serde_json::from_slice(include_bytes!("fixtures/client-startup.json")).unwrap();
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("SKVOZ_RUNTIME_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    value["core"]["url"] = if id == 0 {
        fixture.get("server_url").unwrap_or(&fixture["url"]).clone()
    } else {
        fixture["url"].clone()
    };
    value["core"]["trust"] = json!("managed_ca");
    value["core"]["ca_file"] = fixture["ca"].clone();
    value["core"]["namespace"] = fixture["namespace"].clone();
    value["core"]["username"] = json!(format!("peer{id}"));
    value["core"]["password"] = fixture["passwords"][id.to_string()].clone();
    value["core"]["peer_id"] = json!(id.to_string());
    if id == 0 {
        value["role"] = json!("server");
        value["core"]["allowed_peers"] = json!(["1", "2"]);
        value["core"]["initiate"] = json!([]);
        value["network"]["families"] = json!([]);
        value["network"]["limits"] = serde_json::to_value(Limits::canonical(Role::Server)).unwrap();
        value["server"] = json!({"ipv4":null,"ipv6":null,"dns_servers":[],"allow":[{"cidr":"127.0.0.1/32","protocols":[6],"ports":[port]}],"deny":[],"service_prefixes":[],"lease_store":std::env::temp_dir().join("runtime-access-leases.json"),"server_addresses":[],"management_endpoints":[]});
        value["access"] = json!({"revision":1,"devices":[1,2],"revoke":[]});
    }
    StartupConfig::parse_json(&serde_json::to_vec(&value).unwrap()).unwrap()
}
fn echo(phase: &str, stream: &mut UnixStream) {
    eprintln!("OWNING_PHASE {phase}");
    let bytes: Vec<u8> = (0..256).cycle().take(8192).map(|v| v as u8).collect();
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut written = 0;
    while written < bytes.len() {
        let count = bounded_io(deadline, phase, "write", written, || {
            stream.write(&bytes[written..])
        });
        assert!(count > 0, "echo descriptor closed during write");
        written += count;
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut reply = vec![0; bytes.len()];
    let mut received = 0;
    while received < reply.len() {
        let count = bounded_io(deadline, phase, "read", received, || {
            stream.read(&mut reply[received..])
        });
        assert!(count > 0, "echo descriptor closed during read");
        received += count;
    }
    assert_eq!(reply, bytes);
}
fn bounded_io(
    deadline: Instant,
    phase: &str,
    direction: &str,
    progressed: usize,
    mut operation: impl FnMut() -> io::Result<usize>,
) -> usize {
    loop {
        assert!(
            Instant::now() < deadline,
            "native descriptor progress deadline: phase={phase} direction={direction} bytes={progressed}"
        );
        match operation() {
            Ok(count) => return count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("native descriptor operation failed: {error}"),
        }
    }
}
struct Target {
    port: u16,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
    counters: Arc<[AtomicU64; 4]>,
}
impl Target {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let signal = stop.clone();
        let counters = Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
        let observations = counters.clone();
        let worker = thread::spawn(move || {
            let mut workers = vec![];
            while !signal.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        assert!(workers.len() < 16);
                        observations[0].fetch_add(1, Ordering::Relaxed);
                        let counts = observations.clone();
                        let signal = signal.clone();
                        stream
                            .set_read_timeout(Some(Duration::from_millis(100)))
                            .unwrap();
                        stream
                            .set_write_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        workers.push(thread::spawn(move || {
                            let mut bytes = [0; 16384];
                            while !signal.load(Ordering::Acquire) {
                                match stream.read(&mut bytes) {
                                    Ok(0) => {
                                        counts[3].fetch_add(1, Ordering::Relaxed);
                                        break;
                                    }
                                    Ok(n) => {
                                        counts[1].fetch_add(n as u64, Ordering::Relaxed);
                                        if stream.write_all(&bytes[..n]).is_err() {
                                            break;
                                        }
                                        counts[2].fetch_add(n as u64, Ordering::Relaxed);
                                    }
                                    Err(e)
                                        if matches!(
                                            e.kind(),
                                            std::io::ErrorKind::WouldBlock
                                                | std::io::ErrorKind::TimedOut
                                        ) => {}
                                    Err(_) => break,
                                }
                            }
                        }));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5))
                    }
                    Err(e) => panic!("target accept failed: {e}"),
                }
            }
            for worker in workers {
                worker.join().unwrap();
            }
        });
        Self {
            port,
            stop,
            worker: Some(worker),
            counters,
        }
    }
}
impl Drop for Target {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.worker.take().unwrap().join().unwrap();
    }
}

#[test]
#[ignore = "requires an isolated current TLS NATS fixture"]
fn revoke_actual_native_tcp_and_preserve_unrelated_core_connection_with_stale_admission_denied() {
    let target = Target::new();
    let mut server = Owner::new(configured(0, target.port));
    let mut first = Owner::new(configured(1, target.port));
    let mut healthy = Owner::new(configured(2, target.port));
    for owner in [&mut first, &mut healthy] {
        assert!(
            owner
                .call("START_PROXY", json!({"http_bind":null,"socks_bind":null}))
                .0["error"]
                .is_null()
        );
    }
    eprintln!("OWNING_PHASE open-retired");
    let mut retired = first.open(target.port);
    eprintln!("OWNING_PHASE open-healthy");
    let mut retained = healthy.open(target.port);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        echo("initial-retired", &mut retired);
        echo("initial-healthy", &mut retained);
        assert_eq!(server.tcp_count(), 2);
        eprintln!("OWNING_PHASE update-devices");
        let started = Instant::now();
        let update = json!({"revision":2,"devices":[2],"revoke":[1]});
        assert_eq!(
            server.call("UPDATE_DEVICES", update.clone()).0["result"],
            json!({"revision":2})
        );
        let seconds = started.elapsed().as_secs_f64();
        assert!(seconds <= 3.0, "local native retirement took {seconds}s");
        assert_eq!(
            bounded_io(
                Instant::now() + Duration::from_secs(3),
                "retired-EOF",
                "read",
                0,
                || retired.read(&mut [0])
            ),
            0
        );
        assert_eq!(server.tcp_count(), 1);
        echo("after-revoke-healthy", &mut retained);
        assert_eq!(
            server.call("UPDATE_DEVICES", update).0["result"],
            json!({"revision":2})
        );
        let stale = server.call(
            "UPDATE_DEVICES",
            json!({"revision":1,"devices":[1,2],"revoke":[]}),
        );
        assert_eq!(stale.0["error"], "invalid_state");
        echo("after-stale-healthy", &mut retained);
        let denied = first.call("OPEN_TCP", json!({"host":"127.0.0.1","port":target.port}));
        assert_eq!(denied.0["error"], "network_unavailable");
        assert_eq!(server.tcp_count(), 1);
        println!("LOCAL_REVOKE_PASS seconds={seconds:.6} retained_tcp=1 retired_tcp=0 revision=2");
    }));
    if let Err(error) = outcome {
        eprintln!(
            "OWNING_TARGET accepted={} read_bytes={} echoed_bytes={} eof={}",
            target.counters[0].load(Ordering::Relaxed),
            target.counters[1].load(Ordering::Relaxed),
            target.counters[2].load(Ordering::Relaxed),
            target.counters[3].load(Ordering::Relaxed)
        );
        for (name, owner) in [
            ("server", &mut server),
            ("retired", &mut first),
            ("healthy", &mut healthy),
        ] {
            let status = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                owner
                    .call_budget("STATUS", json!({}), Duration::from_secs(1))
                    .0
            }));
            match status {
                Ok(value) => eprintln!("OWNING_STATUS {name} {value}"),
                Err(_) => eprintln!("OWNING_STATUS {name} unavailable-within-1s"),
            }
        }
        std::panic::resume_unwind(error);
    }
}
