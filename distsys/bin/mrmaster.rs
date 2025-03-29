use distsys::mr;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();

    let master = mr::master::Master::new();
    master.run().await?;

    Ok(())
}
