use clap::Parser;
use tokio::fs::File;
use tokio::io::AsyncWriteExt as _;

#[derive(Parser)]
#[command(name = "mrsequential")]
struct Args {
    app: String,
    input_files: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::try_parse()?;

    println!("app: {}, files: {:?}", args.app, args.input_files);
    let app = distsys::mrapps::get_app(args.app)?;

    let mut intermediate = vec![];
    for file in args.input_files {
        let contents = tokio::fs::read_to_string(file).await?;
        for line in contents.lines() {
            for kv in app.map("key".to_string(), line.to_string()).await? {
                intermediate.push(kv);
            }
        }
    }

    intermediate.sort_by(|a, b| a.key.cmp(&b.key));

    let out_file = "mr-out-0";
    let mut out = File::create(out_file).await?;

    let mut key: Option<&String> = None;
    let mut j = 0;

    for i in 0..=intermediate.len() {
        if let Some(false) = key.map(|k| !(i == intermediate.len() || k != &intermediate[i].key)) {
            let values = intermediate[j..i].iter().map(|kv| kv.value.clone()).collect();
            let output = app.reduce(key.unwrap().clone(), values).await?;
            out.write_all(format!("{} {}\n", key.unwrap(), output).as_bytes()).await?;
            j = i;
        }
        if i < intermediate.len() {
            key = Some(&intermediate[i].key);
        }
    }
    
    Ok(())
}
