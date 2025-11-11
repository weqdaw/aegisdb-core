fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = tonic_build::configure()
        .build_server(true)
        .build_client(true);

    config.compile(
        &[
            "proto/metapb.proto",
            "proto/errorpb.proto",
            "proto/schedulerpb.proto",
        ],
        &["proto"],
    )?;
    Ok(())
}