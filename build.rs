fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 编译所有相关的 proto 文件
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(
            &[
                "proto/tinykvpb.proto",
                "proto/kvrpcpb.proto",
                "proto/metapb.proto",
                "proto/errorpb.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
