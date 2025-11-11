use clap::{Parser, Subcommand};
use aegisdb::{Config, Server, Storage, TieredStorage};
use aegisdb::server::TinyKvService;
use tonic::transport::Server as TonicServer;
use tonic::Request;
use std::net::SocketAddr;
use std::io::{self, Write};
use aegisdb::server::grpc::tinykvpb::tiny_kv_client::TinyKvClient;
use aegisdb::server::grpc::kvrpcpb::{
    RawGetRequest, RawPutRequest, RawDeleteRequest, RawScanRequest,
    GetRequest, ScanRequest, PrewriteRequest, CommitRequest,
    Mutation, Op, Context,
};

#[derive(Parser)]
#[command(name = "aegisdb")]
#[command(about = "AegisDB - A distributed key-value database", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the AegisDB server
    Run {
        /// Server address (default: 127.0.0.1:20160)
        #[arg(short, long, default_value = "127.0.0.1:20160")]
        addr: String,
        
        /// Database path (default: /tmp/aegisdb)
        #[arg(short, long, default_value = "/tmp/aegisdb")]
        db_path: String,

        /// Admin HTTP address (default: 0.0.0.0:8080)
        #[arg(long, default_value = "0.0.0.0:8080")]
        admin_addr: String,

        /// Cluster ID for this process to report (default: 0)
        #[arg(long, default_value = "0")]
        cluster_id: u64,
    },
    /// Start the AegisDB CLI client
    Cli {
        /// Server address to connect to (default: 127.0.0.1:20160)
        #[arg(short, long, default_value = "127.0.0.1:20160")]
        addr: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    
    let cli = Cli::parse();
    
    match cli.command {
        Commands::Run { addr, db_path, admin_addr, cluster_id } => {
            run_server(addr, db_path, admin_addr, cluster_id).await?;
        }
        Commands::Cli { addr } => {
            run_cli(addr).await?;
        }
    }
    
    Ok(())
}

async fn run_server(addr: String, db_path: String, admin_addr: String, cluster_id: u64) -> anyhow::Result<()> {
    println!(" ________  _______   ________  ___  ________  ________  ________     ");
    println!("|\\   __  \\|\\  ___ \\ |\\   ____\\|\\  \\|\\   ____\\|\\   ___ \\|\\   __  \\    ");
    println!("\\ \\  \\|\\  \\ \\   __/|\\ \\  \\___|\\ \\  \\ \\  \\___|\\ \\  \\_|\\ \\ \\  \\|\\ /_   ");
    println!(" \\ \\   __  \\ \\  \\_|/_\\ \\  \\  __\\ \\  \\ \\_____  \\ \\  \\ \\\\ \\ \\   __  \\  ");
    println!("  \\ \\  \\ \\  \\ \\  \\_|\\ \\ \\  \\|\\  \\ \\  \\|____|\\  \\ \\  \\_\\\\ \\ \\  \\|\\  \\ ");
    println!("   \\ \\__\\ \\__\\ \\_______\\ \\_______\\ \\__\\____\\_\\  \\ \\_______\\ \\_______\\");
    println!("    \\|__|\\|__|\\|_______|\\|_______|\\|__|\\_________\\|_______|\\|_______|");
    println!("                                      \\|_________|                   ");
    println!("                                                                     ");
    println!("                                                                     ");
    println!("Starting server on {}...", addr);
    println!("Database path: {}", db_path);
    println!();
    
    // 创建配置
    let mut config = Config::new_default();
    config.db_path = db_path;
    config.validate()?;
    
    // 创建存储
    let storage = TieredStorage::new(&config)?;
    storage.start().await?;
    
    println!("Storage initialized successfully");
    println!();
    
    // 创建服务器
    let server = Server::new(storage);
    // 启动分层存储后台搬迁任务（从 server 中取出存储 Arc）
    {
        let storage_arc = server.storage().clone();
        let interval = config.rebalance_interval;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            loop {
                tick.tick().await;
                if let Err(e) = storage_arc.rebalance_once().await {
                    eprintln!("rebalance_once error: {}", e);
                }
            }
        });
    }
    let service = TinyKvService::new(server);


    // ========== 管理 HTTP 服务与调度组件（用于前端面板） ==========
    use std::sync::Arc;
    use aegisdb::server::admin_http::{AppState, serve};
    use aegisdb::scheduler::cluster::BasicCluster;
    use aegisdb::scheduler::coordinator::Coordinator;
    use aegisdb::scheduler::heartbeat_streams::SimpleHeartbeatStreams;
    use aegisdb::scheduler::operator_controller::OperatorController;
    use aegisdb::scheduler::schedulers::{BalanceRegionScheduler, BalanceLeaderScheduler};
    use aegisdb::raftstore::store_balancer::StoreLoad;

    // BasicCluster
    let cluster = Arc::new(BasicCluster::new());

    // Heartbeat streams and OperatorController
    let hb_streams = Arc::new(SimpleHeartbeatStreams::new());
    let op_controller = Arc::new(OperatorController::new(cluster.clone(), hb_streams.clone()));

    // Coordinator with schedulers
    let coordinator = Arc::new(Coordinator::new(cluster.clone(), op_controller.clone()));
    coordinator.register_scheduler(Box::new(BalanceRegionScheduler::new(std::time::Duration::from_secs(30))));
    coordinator.register_scheduler(Box::new(BalanceLeaderScheduler::new(std::time::Duration::from_secs(30))));
    coordinator.start().await;

    // Store loads view (simple snapshot; expect to be filled by real heartbeats)
    let store_loads = Arc::new(parking_lot::RwLock::new(Vec::<StoreLoad>::new()));

    // cluster id source (from CLI flag)
    let get_cluster_id = {
        let cid = cluster_id;
        Arc::new(move || cid) as Arc<dyn Fn() -> u64 + Send + Sync>
    };

    // Start admin http server in background
    let store_loads_for_admin = store_loads.clone();
    let admin_state = AppState {
        cluster: cluster.clone(),
        coordinator: coordinator.clone(),
        get_cluster_id,
        store_loads: store_loads_for_admin,
    };
    let admin_bind = admin_addr.clone();
    tokio::spawn(async move {
        if let Err(e) = serve(admin_state, &admin_bind).await {
            eprintln!("admin http server failed: {}", e);
        }
    });

    // ========== 真实链路：注册本 Store 并启动心跳，填充 BasicCluster ==========
    {
        use aegisdb::proto::metapb::{Store, StoreState, Region, Peer, RegionEpoch};
        use aegisdb::scheduler::cluster::{StoreInfo, RegionInfo};
        use std::time::Duration;

        // 以 gRPC 端口作为唯一性生成 store_id（127.0.0.1:20160 -> 20160）
        let store_id = addr
            .split(':')
            .last()
            .and_then(|p| p.parse::<u64>().ok())
            .unwrap_or(20160);

        // 注册 Store（UP）
        let store_meta = Store {
            id: store_id,
            address: addr.clone(),
            state: StoreState::Up as i32,
        };
        cluster.put_store(StoreInfo::new(store_meta.clone()));

        // 为每个节点初始化一个基础 Region（leader 在本 store），用于让 UI 有真实链路数据
        // region_id 规则：store_id * 10 + 1，peer_id：store_id * 100 + 1
        let base_region = {
            let region = Region {
                id: store_id * 10 + 1,
                start_key: b"a".to_vec(),
                end_key: b"".to_vec(),
                region_epoch: Some(RegionEpoch { conf_ver: 1, version: 1 }),
                peers: vec![Peer { id: store_id * 100 + 1, store_id }],
            };
            let mut info = RegionInfo::new(
                region,
                Some(Peer { id: store_id * 100 + 1, store_id }),
            );
            info.set_approximate_size(8 * 1024 * 1024);
            info
        };
        cluster.put_region(base_region.clone());

        // 心跳：周期性刷新 Region 和 Store 视图到 BasicCluster 与 store_loads（模拟真实上报）
        let cluster_clone = cluster.clone();
        let store_loads_clone = store_loads.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            // 简单波动的近似 region size
            let mut size: u64 = 8 * 1024 * 1024;
            loop {
                tick.tick().await;

                // 模拟 region size 变化（在 6~14MB 之间抖动）
                let delta: i64 = if (size / (1024 * 1024)) % 2 == 0 { 1024 * 512 } else { -(1024 as i64) * 256 };
                let new_size = (size as i64 + delta).clamp(6 * 1024 * 1024, 14 * 1024 * 1024) as u64;
                size = new_size;

                // 更新 region 视图
                let mut region_updated = base_region.clone();
                region_updated.set_approximate_size(size);
                cluster_clone.put_region(region_updated);

                // 根据 BasicCluster 的统计，更新 store_loads 对外展示（Admin 面板的 /api/storeloads）
                // 注意：BasicCluster 会在 put_region 时自动回填每个 store 的统计
                {
                    let stores = cluster_clone.get_stores();
                    let mut loads = store_loads_clone.write();
                    loads.clear();
                    for s in stores {
                        let mut load = StoreLoad::new(s.id());
                        load.update(s.region_count(), s.leader_count(), s.region_size(), s.leader_size());
                        load.is_up = s.is_up();
                        loads.push(load);
                    }
                }
            }
        });
    }

    // ========== 启动 gRPC 服务器 ==========
    let addr: SocketAddr = addr.parse()?;
    println!("AegisDB gRPC server listening on {}", addr);
    println!("Admin HTTP listening on {}", admin_addr);
    println!("Press Ctrl+C to stop the server");
    println!();

    TonicServer::builder()
        .add_service(aegisdb::server::grpc::tinykvpb::tiny_kv_server::TinyKvServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

async fn run_cli(addr: String) -> anyhow::Result<()> {
    let server_addr = format!("http://{}", addr);
    
    println!("=== AegisDB CLI ===");
    println!("Connecting to {}...", server_addr);
    
    // 连接服务器
    let mut client = match TinyKvClient::connect(server_addr.clone()).await {
        Ok(c) => {
            println!("Connected successfully!");
            c
        }
        Err(e) => {
            eprintln!("Failed to connect to server: {}", e);
            eprintln!("Make sure the server is running: aegisdb run");
            return Err(e.into());
        }
    };
    
    println!("\nAvailable commands:");
    println!("RawKV Commands:");
    println!("  GET <key> [cf]          - Get value for key (default CF: 'default')");
    println!("  SET <key> <value> [cf] - Set key-value pair (default CF: 'default')");
    println!("  DEL <key> [cf]         - Delete key (default CF: 'default')");
    println!("  SCAN <start_key> <limit> [cf] - Scan keys (default CF: 'default')");
    println!();
    println!("Transaction Commands:");
    println!("  KVGET <key> <version>  - Transactional get at version");
    println!("  KVSCAN <start_key> <limit> <version> - Transactional scan at version");
    println!("  KVPREWRITE <primary_key> <start_version> <lock_ttl> <key1> <value1> [key2 value2 ...] - Prewrite transaction");
    println!("  KVCOMMIT <start_version> <commit_version> <key1> [key2 ...] - Commit transaction");
    println!();
    println!("Other Commands:");
    println!("  HELP/?                 - Show this help message");
    println!("  QUIT/EXIT/Q            - Exit CLI");
    println!();
    
    // REPL 循环
    loop {
        print!("aegisdb> ");
        io::stdout().flush()?;
        
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        
        let parts: Vec<&str> = input.trim().split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }
        
        match parts[0].to_uppercase().as_str() {
            "GET" => {
                if parts.len() < 2 {
                    println!("Usage: GET <key> [cf]");
                    continue;
                }
                
                let key = parts[1].as_bytes().to_vec();
                let cf = parts.get(2).map(|s| s.to_string()).unwrap_or_else(|| "default".to_string());
                
                let req = Request::new(RawGetRequest {
                    context: Some(Context {
                        region_id: 1,
                        region_epoch: None,
                        peer: None,
                        term: 0,
                    }),
                    key,
                    cf,
                });
                
                match client.raw_get(req).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if resp.not_found {
                            println!("(nil)");
                        } else if !resp.error.is_empty() {
                            println!("Error: {}", resp.error);
                        } else {
                            let value = String::from_utf8_lossy(&resp.value);
                            println!("\"{}\"", value);
                        }
                    }
                    Err(e) => println!("Error: {}", e),
                }
            }
            
            "SET" => {
                if parts.len() < 3 {
                    println!("Usage: SET <key> <value> [cf]");
                    continue;
                }
                
                let key = parts[1].as_bytes().to_vec();
                let value = parts[2].as_bytes().to_vec();
                let cf = parts.get(3).map(|s| s.to_string()).unwrap_or_else(|| "default".to_string());
                
                let req = Request::new(RawPutRequest {
                    context: Some(Context {
                        region_id: 1,
                        region_epoch: None,
                        peer: None,
                        term: 0,
                    }),
                    key,
                    value,
                    cf,
                });
                
                match client.raw_put(req).await {
                    Ok(_) => println!("OK"),
                    Err(e) => println!("Error: {}", e),
                }
            }
            
            "DEL" | "DELETE" => {
                if parts.len() < 2 {
                    println!("Usage: DEL <key> [cf]");
                    continue;
                }
                
                let key = parts[1].as_bytes().to_vec();
                let cf = parts.get(2).map(|s| s.to_string()).unwrap_or_else(|| "default".to_string());
                
                let req = Request::new(RawDeleteRequest {
                    context: Some(Context {
                        region_id: 1,
                        region_epoch: None,
                        peer: None,
                        term: 0,
                    }),
                    key,
                    cf,
                });
                
                match client.raw_delete(req).await {
                    Ok(_) => println!("OK"),
                    Err(e) => println!("Error: {}", e),
                }
            }
            
            "SCAN" => {
                if parts.len() < 3 {
                    println!("Usage: SCAN <start_key> <limit> [cf]");
                    continue;
                }
                
                let start_key = parts[1].as_bytes().to_vec();
                let limit: u32 = parts[2].parse().unwrap_or(10);
                let cf = parts.get(3).map(|s| s.to_string()).unwrap_or_else(|| "default".to_string());
                
                let req = Request::new(RawScanRequest {
                    context: Some(Context {
                        region_id: 1,
                        region_epoch: None,
                        peer: None,
                        term: 0,
                    }),
                    start_key,
                    limit,
                    cf,
                });
                
                match client.raw_scan(req).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if resp.kvs.is_empty() {
                            println!("(empty list or set)");
                        } else {
                            for (i, kv) in resp.kvs.iter().enumerate() {
                                let key = String::from_utf8_lossy(&kv.key);
                                let value = String::from_utf8_lossy(&kv.value);
                                println!("{}) \"{}\" -> \"{}\"", i + 1, key, value);
                            }
                        }
                    }
                    Err(e) => println!("Error: {}", e),
                }
            }
            
            "QUIT" | "EXIT" | "Q" => {
                println!("Bye!");
                break;
            }
            
            "KVGET" => {
                if parts.len() < 3 {
                    println!("Usage: KVGET <key> <version>");
                    continue;
                }
                
                let key = parts[1].as_bytes().to_vec();
                let version: u64 = match parts[2].parse() {
                    Ok(v) => v,
                    Err(_) => {
                        println!("Error: Invalid version number");
                        continue;
                    }
                };
                
                let req = Request::new(GetRequest {
                    context: Some(Context {
                        region_id: 1,
                        region_epoch: None,
                        peer: None,
                        term: 0,
                    }),
                    key,
                    version,
                });
                
                match client.kv_get(req).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if resp.not_found {
                            println!("(nil)");
                        } else if resp.error.is_some() {
                            println!("Error: Transaction conflict or locked");
                        } else {
                            let value = String::from_utf8_lossy(&resp.value);
                            println!("\"{}\"", value);
                        }
                    }
                    Err(e) => println!("Error: {}", e),
                }
            }
            
            "KVSCAN" => {
                if parts.len() < 4 {
                    println!("Usage: KVSCAN <start_key> <limit> <version>");
                    continue;
                }
                
                let start_key = parts[1].as_bytes().to_vec();
                let limit: u32 = parts[2].parse().unwrap_or(10);
                let version: u64 = match parts[3].parse() {
                    Ok(v) => v,
                    Err(_) => {
                        println!("Error: Invalid version number");
                        continue;
                    }
                };
                
                let req = Request::new(ScanRequest {
                    context: Some(Context {
                        region_id: 1,
                        region_epoch: None,
                        peer: None,
                        term: 0,
                    }),
                    start_key,
                    limit,
                    version,
                });
                
                match client.kv_scan(req).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if resp.pairs.is_empty() {
                            println!("(empty list or set)");
                        } else {
                            for (i, kv) in resp.pairs.iter().enumerate() {
                                let key = String::from_utf8_lossy(&kv.key);
                                if kv.error.is_some() {
                                    println!("{}) \"{}\" -> (locked or error)", i + 1, key);
                                } else {
                                    let value = String::from_utf8_lossy(&kv.value);
                                    println!("{}) \"{}\" -> \"{}\"", i + 1, key, value);
                                }
                            }
                        }
                    }
                    Err(e) => println!("Error: {}", e),
                }
            }
            
            "KVPREWRITE" => {
                if parts.len() < 6 {
                    println!("Usage: KVPREWRITE <primary_key> <start_version> <lock_ttl> <key1> <value1> [key2 value2 ...]");
                    continue;
                }
                
                let primary_key = parts[1].as_bytes().to_vec();
                let start_version: u64 = match parts[2].parse() {
                    Ok(v) => v,
                    Err(_) => {
                        println!("Error: Invalid start_version number");
                        continue;
                    }
                };
                let lock_ttl: u64 = parts[3].parse().unwrap_or(3000);
                
                let mut mutations = Vec::new();
                let mut i = 4;
                while i < parts.len() {
                    if i + 1 >= parts.len() {
                        println!("Error: Missing value for key {}", parts[i]);
                        break;
                    }
                    mutations.push(Mutation {
                        op: Op::Put as i32,
                        key: parts[i].as_bytes().to_vec(),
                        value: parts[i + 1].as_bytes().to_vec(),
                    });
                    i += 2;
                }
                
                if mutations.is_empty() {
                    println!("Error: No mutations provided");
                    continue;
                }
                
                let req = Request::new(PrewriteRequest {
                    context: Some(Context {
                        region_id: 1,
                        region_epoch: None,
                        peer: None,
                        term: 0,
                    }),
                    mutations,
                    primary_lock: primary_key,
                    start_version,
                    lock_ttl,
                });
                
                match client.kv_prewrite(req).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if resp.errors.is_empty() {
                            println!("OK");
                        } else {
                            println!("Error: {} mutation(s) failed", resp.errors.len());
                        }
                    }
                    Err(e) => println!("Error: {}", e),
                }
            }
            
            "KVCOMMIT" => {
                if parts.len() < 4 {
                    println!("Usage: KVCOMMIT <start_version> <commit_version> <key1> [key2 ...]");
                    continue;
                }
                
                let start_version: u64 = match parts[1].parse() {
                    Ok(v) => v,
                    Err(_) => {
                        println!("Error: Invalid start_version number");
                        continue;
                    }
                };
                let commit_version: u64 = match parts[2].parse() {
                    Ok(v) => v,
                    Err(_) => {
                        println!("Error: Invalid commit_version number");
                        continue;
                    }
                };
                
                let keys: Vec<Vec<u8>> = parts[3..].iter().map(|s| s.as_bytes().to_vec()).collect();
                
                let req = Request::new(CommitRequest {
                    context: Some(Context {
                        region_id: 1,
                        region_epoch: None,
                        peer: None,
                        term: 0,
                    }),
                    start_version,
                    keys,
                    commit_version,
                });
                
                match client.kv_commit(req).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if resp.error.is_some() {
                            println!("Error: Commit failed");
                        } else {
                            println!("OK");
                        }
                    }
                    Err(e) => println!("Error: {}", e),
                }
            }
            
            "HELP" | "?" => {
                println!("Available commands:");
                println!("RawKV Commands:");
                println!("  GET <key> [cf]          - Get value for key");
                println!("  SET <key> <value> [cf] - Set key-value pair");
                println!("  DEL <key> [cf]         - Delete key");
                println!("  SCAN <start_key> <limit> [cf] - Scan keys");
                println!();
                println!("Transaction Commands:");
                println!("  KVGET <key> <version>  - Transactional get at version");
                println!("  KVSCAN <start_key> <limit> <version> - Transactional scan at version");
                println!("  KVPREWRITE <primary_key> <start_version> <lock_ttl> <key1> <value1> [key2 value2 ...] - Prewrite transaction");
                println!("  KVCOMMIT <start_version> <commit_version> <key1> [key2 ...] - Commit transaction");
                println!();
                println!("Other Commands:");
                println!("  HELP/?                 - Show this help message");
                println!("  QUIT/EXIT/Q            - Exit CLI");
            }
            
            _ => {
                println!("Unknown command: {}. Type HELP for available commands.", parts[0]);
            }
        }
    }
    
    Ok(())
}