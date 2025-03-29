use crate::mr::MRApp;

pub async fn run(app: Box<dyn MRApp>, input_files: Vec<String>) -> Result<(), anyhow::Error> {
    for file in input_files {
        let contents = tokio::fs::read_to_string(file).await?;
        for line in contents.lines() {
            for kv in app.map("key".to_string(), line.to_string()).await? {
                println!("{}: {}", kv.key, kv.value);
            }
        }
    }

    Ok(())
}
