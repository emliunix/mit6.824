pub mod proto {
    tonic::include_proto!("mr");
}

#[link(name = "c")]
unsafe extern "C" {
    fn geteuid() -> u32;
    fn getegid() -> u32;
}

pub(crate) fn master_sock() -> String {
    format!("/tmp/824-mr-{}", unsafe {geteuid()})
}
