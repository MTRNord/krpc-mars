fn main() {
    println!("cargo:rerun-if-changed=protos/krpc.proto");
    println!("cargo:rerun-if-changed=src/krpc.rs");
    protobuf_codegen::Codegen::new()
        .out_dir("src/")
        .includes(["protos"])
        .inputs(["protos/krpc.proto"])
        .run()
        .expect("protoc");
}
