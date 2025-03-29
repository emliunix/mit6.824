const PG_FILES: &[&'static str] = &[
    "../src/main/pg-being_ernest.txt",
    "../src/main/pg-dorian_gray.txt",
    "../src/main/pg-frankenstein.txt",
    "../src/main/pg-grimm.txt",
    "../src/main/pg-huckleberry_finn.txt",
    "../src/main/pg-metamorphosis.txt",
    "../src/main/pg-sherlock_holmes.txt",
    "../src/main/pg-tom_sawyer.txt",
];

#[tokio::test]
async fn test_wc() {
    let app = crate::mrapps::wc::WC;
    let input_files = PG_FILES.iter().map(|s| s.to_string()).collect();
    crate::mrsequential::run(app, input_files).await.unwrap();
}
