use aegisdb::pd::{PdConfig, PdService, PdState};
use aegisdb::pd::http::{serve, AppState};
use aegisdb::proto::schedulerpb::pd_server::PdServer;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tonic::transport::Server;

#[derive(Parser, Debug)]
#[command(name = "pd_server")]
#[command(about = "AegisDB Placement Driver")]
struct Args {
    #[arg(long, default_value = "1")]
    cluster_id: u64,
    #[arg(long, default_value = "127.0.0.1:2379")]
    grpc_addr: String,
    #[arg(long, default_value = "0.0.0.0:8080")]
    http_addr: String,
    #[arg(long, default_value = "./pd-data")]
    data_dir: PathBuf,
    #[arg(long, value_delimiter = ',', default_value = "http://127.0.0.1:2379")]
    advertise_client_urls: Vec<String>,
    #[arg(long, value_delimiter = ',', default_value = "http://127.0.0.1:2380")]
    advertise_peer_urls: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info"),
    )
    .init();

    let args = Args::parse();

    let config = PdConfig::new(
        args.cluster_id,
        args.grpc_addr.clone(),
        args.http_addr.clone(),
        args.data_dir,
        args.advertise_client_urls,
        args.advertise_peer_urls,
    );

    let state = Arc::new(PdState::new(config));
    state.start().await;

    let http_state = AppState {
        pd_state: state.clone(),
    };
    let http_addr = args.http_addr.clone();
    tokio::spawn(async move {
        if let Err(e) = serve(http_state, &http_addr).await {
            log::error!("pd http server error: {}", e);
        }
    });

    let service = PdService::new(state.clone());
    let addr: SocketAddr = args.grpc_addr.parse()?;

    log::info!("PD listening on gRPC {}", addr);
    log::info!("PD HTTP API listening on {}", args.http_addr);

    Server::builder()
        .add_service(PdServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

