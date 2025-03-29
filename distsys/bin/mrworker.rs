use distsys::mr;
use clap::Parser;

#[derive(Parser)]
#[command(name = "mrworker")]
struct Args {
    app: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let worker = mr::worker::Worker{};
    worker.run(args.app)
        .await?;

    Ok(())
}
