fn main() {
    tonic_build::configure()
        .compile_protos(&["proto/mr.proto"], &["proto"])
        .unwrap();
}
