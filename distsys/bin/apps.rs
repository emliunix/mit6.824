use distsys::mr::MRApp;

pub fn get_app(app: String) -> Result<Box<dyn MRApp>, anyhow::Error> {
    match app.as_str() {
        "wc" => Ok(Box::new(distsys::mrapps::wc::WC)),
        _ => Err(anyhow::anyhow!("Unknown app: {}", app)),
    }
}
