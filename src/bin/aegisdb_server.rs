use aegisdb::{Config, StandaloneStorage, Server, Storage};
use aegisdb::server::TinyKvService;
use tonic::transport::Server as TonicServer;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    
    // 解析命令行参数
    let args: Vec<String> = std::env::args().collect();
    let addr = if args.len() > 1 {
        args[1].clone()
    } else {
        "127.0.0.1:20160".to_string()
    };
    
    println!("=== AegisDB Server ===");
    println!("Starting server on {}...", addr);
    
    // 创建配置和存储
    let config = Config::new_default();
    config.validate()?;
    
    let storage = StandaloneStorage::new(&config)?;
    storage.start().await?;
    
    println!("Storage initialized successfully");
    
    // 创建服务器
    let server = Server::new(storage);
    let service = TinyKvService::new(server);
    
    // 启动 gRPC 服务器
    let addr: SocketAddr = addr.parse()?;
    println!("AegisDB server listening on {}", addr);
    
    TonicServer::builder()
        .add_service(aegisdb::server::grpc::tinykvpb::tiny_kv_server::TinyKvServer::new(service))
        .serve(addr)
        .await?;
    
    Ok(())
}