// Compiles the vendored .proto files with protox (pure Rust), so building
// requires nothing but cargo: no protoc, no Go toolchain.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = ["proto/gnmi.proto", "proto/tunnel.proto"];
    for p in protos.iter().chain(&["proto/gnmi_ext.proto"]) {
        println!("cargo:rerun-if-changed={p}");
    }
    let fds = protox::compile(protos, ["proto"])?;

    // The descriptor set backs the protojson/prototext output formats.
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    std::fs::write(out.join("descriptor.bin"), prost::Message::encode_to_vec(&fds))?;

    tonic_prost_build::configure()
        // Tunnel payloads are forwarded as-is: avoid a copy per frame.
        .bytes(".grpctunnel.Data.data")
        // Ordered keys give deterministic path rendering.
        .btree_map(".gnmi.PathElem.key")
        .compile_fds(fds)?;
    Ok(())
}
