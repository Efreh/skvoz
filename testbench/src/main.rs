use skvoz_testbench::{mesh, scenarios};
#[tokio::main]
async fn main() -> Result<(), skvoz_testbench::BenchError> {
    let args: Vec<_> = std::env::args().collect();
    match args.get(1).map(String::as_str).unwrap_or("demo") {
        "load" => {
            mesh::run(
                "load",
                mesh::LoadOptions {
                    clients: args[2].parse()?,
                    streams_per_client: args[3].parse()?,
                    active_per_client: args[4].parse()?,
                    bytes: args[5].parse()?,
                },
            )
            .await
        }
        "tcp" => skvoz_testbench::tcp_demo::run().await,
        _ => scenarios::exchange("demo", 4, 32768).await,
    }
}
