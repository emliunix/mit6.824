use clap::Parser;
use distsys::mr;

#[derive(clap::Parser)]
#[command(name = "mrmaster")]
struct Args {
    inputs: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let master = mr::master::Master::new();
    master.run(args.inputs).await?;

    Ok(())
}
