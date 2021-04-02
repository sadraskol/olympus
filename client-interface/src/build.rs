extern crate protoc_rust;

fn main() {
    protoc_rust::Codegen::new()
        .out_dir("src")
        .inputs(&["proto/queries.proto"])
        .include("proto")
        .run()
        .expect("Running protoc failed.");
}
