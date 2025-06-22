fn main() {
    protobuf_codegen::Codegen::new()
        .pure()
        .cargo_out_dir("protos")
        .include("src/pprof")
        .input("src/pprof/profile.proto")
        .run_from_script();
}
