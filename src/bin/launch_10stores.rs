use aegisdb::raftstore::scheduler_client::{Client as PdClientImpl, SchedulerClient};
use aegisdb::proto::metapb::{Peer, Region, RegionEpoch, Store, StoreState};
use aegisdb::proto::schedulerpb::StoreStats;
use clap::Parser;
use std::sync::Arc;
use std::future;

#[derive(Parser, Debug)]
#[command(name = "launch_10stores")]
#[command(about = "Launch 10 store clients that register and heartbeat to PD")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:2379")]
    pd: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let pd_endpoint = args.pd.clone();

    log::info!("Connecting 10 stores to PD at {}", pd_endpoint);

    // Create 10 clients (store_id = 1..=10)
    let mut clients: Vec<Arc<dyn SchedulerClient>> = Vec::new();
    for store_id in 1u64..=10 {
        let client = PdClientImpl::connect(vec![pd_endpoint.clone()], store_id, format!("store-{}", store_id)).await?;
        let client: Arc<dyn SchedulerClient> = Arc::new(client);
        clients.push(client);
    }

    // Bootstrap and put_store for each
    for (idx, client) in clients.iter().enumerate() {
        let store_id = (idx as u64) + 1;
        let store = Store {
            id: store_id,
            address: format!("127.0.0.1:20{:03}", 160 + store_id),
            state: StoreState::Up as i32,
        };
        // First node bootstraps, others put_store
        if store_id == 1 {
            let _ = client.bootstrap(&store).await?;
        } else {
            client.put_store(&store).await?;
        }
    }

    // Spawn heartbeat loops for each store
    for (idx, client) in clients.iter().enumerate() {
        let store_id = (idx as u64) + 1;
        let client = client.clone();

        // For visibility, create one region per store and send region heartbeats
        let region = Region {
            id: 10_000 + store_id,
            start_key: format!("store{}_start", store_id).into_bytes(),
            end_key: format!("store{}_end", store_id).into_bytes(),
            region_epoch: Some(RegionEpoch { conf_ver: 1, version: 1 }),
            peers: vec![Peer { id: 1_000_000 + store_id, store_id }],
        };
        let leader = Peer { id: 1_000_000 + store_id, store_id };

        tokio::spawn(async move {
            // region heartbeat stream handler was set in Client::connect; we just send periodic heartbeats
            loop {
                // Store heartbeat
                let stats = StoreStats {
                    store_id,
                    capacity: 100 * 1024 * 1024,
                    available: 80 * 1024 * 1024,
                    used_size: (20 + store_id) * 1024 * 1024,
                    region_count: 1,
                    leader_count: 1,
                    healthy: true,
                    avg_resp_ms: 5 + (store_id % 5),
                    error_count: (store_id % 3) as u64,
                    mem_total: 16 * 1024 * 1024 * 1024,
                    mem_used: ((4 + store_id) * 1024 * 1024 * 1024),
                    disk_total: 256 * 1024 * 1024 * 1024,
                    disk_used: ((20 + store_id) * 1024 * 1024 * 1024),
                    network_state: if store_id % 4 == 0 { "congested".into() } else { "normal".into() },
                };
                if let Err(err) = client.store_heartbeat(&stats).await {
                    log::error!("[store {}] store_heartbeat error: {}", store_id, err);
                }

                // Region heartbeat
                let hb = aegisdb::proto::schedulerpb::RegionHeartbeatRequest {
                    header: Some(client.request_header()),
                    region: Some(region.clone()),
                    leader: Some(leader.clone()),
                    pending_peers: vec![],
                    approximate_size: 1024,
                };
                if let Err(err) = client.region_heartbeat(hb) {
                    log::error!("[store {}] region_heartbeat error: {}", store_id, err);
                }

                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        });
    }

    log::info!("Launched 10 stores; press Ctrl+C to exit. PD HTTP should show them.");
    // Run forever
    future::pending::<()>().await;
    Ok(())
}

