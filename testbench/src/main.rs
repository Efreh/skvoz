use skvoz_testbench::scenarios;

#[tokio::main]
async fn main() -> Result<(), skvoz_testbench::BenchError> {
    println!("User connector <-> Core <-> TLS-first NATS <-> Core <-> Consumer connector");
    scenarios::exchange("demo", 4, 32768).await
}
