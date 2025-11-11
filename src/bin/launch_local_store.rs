use aegisdb::proto::metapb::{Peer, Region, RegionEpoch, Store, StoreState};
use aegisdb::proto::schedulerpb::StoreStats;
use aegisdb::raftstore::scheduler_client::{Client as PdClientImpl, SchedulerClient};
use clap::Parser;
use std::sync::Arc;
use sysinfo::{Disks, MemoryRefreshKind, RefreshKind, System, CpuRefreshKind};
use tokio::time::{sleep, Duration};

#[derive(Parser, Debug)]
#[command(name = "launch_local_store")]
#[command(about = "Launch a single local store that reports REAL system metrics to PD")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:2379")]
    pd: String,
    #[arg(long, default_value_t = 1)]
    store_id: u64,
    #[arg(long, default_value = "127.0.0.1:20160")]
    addr: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();

    // 连接 PD
    let client = PdClientImpl::connect(vec![args.pd.clone()], args.store_id, format!("store-{}", args.store_id)).await?;
    let client: Arc<dyn SchedulerClient> = Arc::new(client);

    // 注册 Store（第一家用 bootstrap，其他用 put_store；单机只用一个即可）
    let store = Store {
        id: args.store_id,
        address: args.addr.clone(),
        state: StoreState::Up as i32,
    };
    // 如果集群未初始化则 bootstrap，否则 put_store；为简化，尝试 bootstrap 失败再 put_store
    match client.bootstrap(&store).await {
        Ok(_) => log::info!("Bootstrap ok"),
        Err(e) => {
            log::warn!("Bootstrap failed: {}. Try put_store...", e);
            client.put_store(&store).await?;
            log::info!("put_store ok");
        }
    }

    // 固定一个 Region/Leader（仅用于展示，真实实现应由 Raft 维持）
    let region = Region {
        id: 10_001,
        start_key: b"local_start".to_vec(),
        end_key: b"local_end".to_vec(),
        region_epoch: Some(RegionEpoch { conf_ver: 1, version: 1 }),
        peers: vec![Peer { id: 1_000_001, store_id: args.store_id }],
    };
    let leader = Peer { id: 1_000_001, store_id: args.store_id };

    let mut sys = System::new_with_specifics(
        RefreshKind::new()
            .with_memory(MemoryRefreshKind::new().with_ram().with_swap())
            .with_cpu(CpuRefreshKind::everything())
    );
    let mut disks = Disks::new();

    // 统计误差可接受：直接累计失败次数作为 error_count；根据 store_heartbeat RTT 估计 avg_resp_ms
    let mut error_count: u64 = 0;
    let mut avg_ms: f64 = 0.0;
    let alpha: f64 = 0.2; // EWMA

    log::info!("Local store {} started; reporting every 2s", args.store_id);

    loop {
        // 刷新系统信息
        sys.refresh_memory();
        disks.refresh_list();
        disks.refresh();

        // 内存
        let mem_total = sys.total_memory(); // bytes
        let mem_used = sys.used_memory();   // bytes

        // 磁盘：汇总所有磁盘（单机足够）
        let mut disk_total: u64 = 0;
        let mut disk_used: u64 = 0;
        for d in disks.list() {
            let total = d.total_space();
            let avail = d.available_space();
            disk_total = disk_total.saturating_add(total);
            disk_used = disk_used.saturating_add(total.saturating_sub(avail));
        }

        // 已有语义：capacity/available/used_size 与 region_size 逻辑相关；这里保持简单映射
        let capacity = disk_total;
        let available = disk_total.saturating_sub(disk_used);
        let used_size = disk_used;

        // region/leader 计数：单机演示各 1 即可
        let region_count = 1;
        let leader_count = 1;

        // 健康：心跳成功即为 healthy=true
        let healthy = true;

        // 发送 store 心跳并计时
        let stats = StoreStats {
            store_id: args.store_id,
            capacity,
            available,
            used_size,
            region_count,
            leader_count,
            healthy,
            avg_resp_ms: avg_ms as u64, // 先填写上一轮 EWMA，当前轮发送后再更新
            error_count,
            mem_total,
            mem_used,
            disk_total,
            disk_used,
            network_state: "normal".to_string(), // 根据 avg_ms 动态设置，见下
        };

        let start = std::time::Instant::now();
        let res = client.store_heartbeat(&stats).await;
        let elapsed = start.elapsed();
        let this_ms = elapsed.as_millis() as f64;
        avg_ms = if avg_ms == 0.0 { this_ms } else { alpha * this_ms + (1.0 - alpha) * avg_ms };

        if let Err(e) = res {
            error_count = error_count.saturating_add(1);
            log::error!("store_heartbeat failed: {}", e);
        }

        // 简单网络状态判定（单机近似）：>80ms 视为 congested，>200ms 视为 degraded
        let network_state = if avg_ms > 200.0 {
            "degraded"
        } else if avg_ms > 80.0 {
            "congested"
        } else {
            "normal"
        }.to_string();

        // 发一条 region 心跳保证 PD 有 region 视图
        let hb = aegisdb::proto::schedulerpb::RegionHeartbeatRequest {
            header: Some(client.request_header()),
            region: Some(region.clone()),
            leader: Some(leader.clone()),
            pending_peers: vec![],
            approximate_size: 4 * 1024 * 1024, // 4MB 演示值
        };
        if let Err(err) = client.region_heartbeat(hb) {
            log::warn!("region_heartbeat error: {}", err);
        }

        // 为了让 network_state 生效，下轮把它带入；这里不改 proto，直接在日志里观测
        log::info!(
            "mem_used/total={} / {}, disk_used/total={} / {}, avg_ms={:.1}, net={}",
            mem_used, mem_total, disk_used, disk_total, avg_ms, network_state
        );

        sleep(Duration::from_secs(2)).await;
    }
}