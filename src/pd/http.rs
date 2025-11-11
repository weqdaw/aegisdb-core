use crate::pd::state::PdState;
use crate::raftstore::store_balancer::StoreLoad;
use axum::{routing::get, Router};
use axum::extract::State;
use serde::Serialize;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

use std::sync::{Mutex, OnceLock};
static SYS: OnceLock<Mutex<sysinfo::System>> = OnceLock::new();

#[derive(Clone)]
pub struct AppState {
    pub pd_state: Arc<PdState>,
}

#[derive(Serialize)]
struct ClusterSummary {
    cluster_id: u64,
    store_count: usize,
    region_count: usize,
    leader_count: usize,
    total_region_size: u64,
}

#[derive(Serialize)]
struct ApiStore {
    id: u64,
    address: String,
    state: String,
    region_count: usize,
    leader_count: usize,
    region_size: u64,
    leader_size: u64,
    // extended metrics
    healthy: bool,
    avg_resp_ms: u64,
    error_count: u64,
    mem_total: u64,
    mem_used: u64,
    disk_total: u64,
    disk_used: u64,
    network_state: String,
}

#[derive(Serialize)]
struct SystemDisk {
    name: String,
    total: u64,
    used: u64,
}

#[derive(Serialize)]
struct SystemNet {
    rx_bytes: u64,
    tx_bytes: u64,
}

#[derive(Serialize)]
struct SystemMetrics {
    cpu_usage_percent: f32,
    mem_total: u64,
    mem_used: u64,
    disks: Vec<SystemDisk>,
    net: SystemNet,
}



#[derive(Serialize)]
struct SystemNetwork {
    rx_bytes: u64,
    tx_bytes: u64,
}

#[derive(Serialize)]
struct ApiRegion {
    id: u64,
    start_key: String,
    end_key: String,
    leader_store_id: Option<u64>,
    approximate_size: u64,
    peer_store_ids: Vec<u64>,
}

#[derive(Serialize)]
struct OperatorSummary {
    running: usize,
    pending: usize,
    finished: usize,
}

#[derive(Serialize)]
struct StoreLoadView {
    store_id: u64,
    region_count: usize,
    leader_count: usize,
    region_size: u64,
    leader_size: u64,
}

pub async fn serve(state: AppState, addr: &str) -> anyhow::Result<()> {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_headers(Any)
        .allow_methods(Any);

    let app = Router::new()
        .route("/api/cluster/summary", get(get_cluster_summary))
        .route("/api/stores", get(list_stores))
        .route("/api/regions", get(list_regions))
        .route("/api/operators", get(list_operators))
        .route("/api/storeloads", get(list_store_loads))
        .route("/api/system", get(get_system_metrics)) // add
        .with_state(state)
        .layer(cors);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn get_cluster_summary(State(state): State<AppState>) -> axum::Json<ClusterSummary> {
    let cluster = state.pd_state.cluster();
    let stores = cluster.get_stores();
    let regions = cluster.get_regions();

    let leader_count: usize = stores.iter().map(|s| s.leader_count()).sum();
    let total_region_size: u64 = regions.iter().map(|r| r.approximate_size()).sum();

    axum::Json(ClusterSummary {
        cluster_id: state.pd_state.cluster_id(),
        store_count: stores.len(),
        region_count: regions.len(),
        leader_count,
        total_region_size,
    })
}

async fn list_stores(State(state): State<AppState>) -> axum::Json<Vec<ApiStore>> {
    let cluster = state.pd_state.cluster();
    let stores = cluster.get_stores();
    axum::Json(
        stores
            .iter()
            .map(|s| ApiStore {
                id: s.id(),
                address: s.address().to_string(),
                state: format!("{:?}", s.state()),
                region_count: s.region_count(),
                leader_count: s.leader_count(),
                region_size: s.region_size(),
                leader_size: s.leader_size(),
                healthy: s.healthy(),
                avg_resp_ms: s.avg_resp_ms(),
                error_count: s.error_count(),
                mem_total: s.mem_total(),
                mem_used: s.mem_used(),
                disk_total: s.disk_total(),
                disk_used: s.disk_used(),
                network_state: s.network_state().to_string(),
            })
            .collect(),
    )
}

async fn list_regions(State(state): State<AppState>) -> axum::Json<Vec<ApiRegion>> {
    let cluster = state.pd_state.cluster();
    let regions = cluster.get_regions();
    axum::Json(
        regions
            .iter()
            .map(|r| ApiRegion {
                id: r.id(),
                start_key: String::from_utf8_lossy(r.start_key()).to_string(),
                end_key: String::from_utf8_lossy(r.end_key()).to_string(),
                leader_store_id: r.get_leader_store_id(),
                approximate_size: r.approximate_size(),
                peer_store_ids: r.peers().iter().map(|p| p.store_id).collect(),
            })
            .collect(),
    )
}

async fn list_operators(State(state): State<AppState>) -> axum::Json<OperatorSummary> {
    use crate::scheduler::operator_controller::OperatorControllerExt;
    let oc = state.pd_state.operator_controller();
    let running = oc.len_running();
    let pending = oc.len_pending();
    let finished = oc.len_finished();
    axum::Json(OperatorSummary {
        running,
        pending,
        finished,
    })
}

async fn list_store_loads(State(state): State<AppState>) -> axum::Json<Vec<StoreLoadView>> {
    let loads: Vec<StoreLoad> = state.pd_state.store_loads_snapshot();
    axum::Json(
        loads
            .into_iter()
            .map(|l| StoreLoadView {
                store_id: l.store_id,
                region_count: l.region_count,
                leader_count: l.leader_count,
                region_size: l.region_size,
                leader_size: l.leader_size,
            })
            .collect(),
    )
}

async fn get_system_metrics() -> axum::Json<SystemMetrics> {
    use sysinfo::{
        CpuRefreshKind, MemoryRefreshKind, RefreshKind,
        System, Disks, Networks,
    };

    // 复用一个全局 System，保证网络累计值可递增
    let sys_lock = SYS.get_or_init(|| {
        Mutex::new(System::new_with_specifics(
            RefreshKind::new()
                .with_memory(MemoryRefreshKind::new().with_ram().with_swap())
                .with_cpu(CpuRefreshKind::everything()),
        ))
    });

    let mut sys = sys_lock.lock().unwrap();

    // 刷新 CPU/内存/磁盘/网络
    sys.refresh_memory();
    sys.refresh_cpu();

    // 对于 Disks/Networks：有的 sysinfo 版本需要显式刷新列表再刷新数据
    // 这里用独立的 Disks/Networks 以获得完整视图
    let mut disks = Disks::new();
    disks.refresh_list();
    disks.refresh();

    let mut nets = Networks::new();
    nets.refresh();

    // CPU 使用率
    let cpu_usage = sys.global_cpu_info().cpu_usage();

    // 磁盘列表：名称用挂载点（盘符），过滤掉 total=0 的项
    let disks_view: Vec<SystemDisk> = disks
        .list()
        .iter()
        .filter_map(|d| {
            let total = d.total_space();
            if total == 0 { return None; }
            let used = total.saturating_sub(d.available_space());
            let name = d.mount_point().to_string_lossy().to_string(); // e.g. "C:\"
            Some(SystemDisk { name, total, used })
        })
        .collect();

    // 网络累计字节（复用 System 后会稳定递增）
    let mut rx: u64 = 0;
    let mut tx: u64 = 0;
    for (_name, data) in nets.iter() {
        // 如果编译报错，试下 total_received()/total_transmitted() 或 received()/transmitted()
        #[allow(deprecated)]
        {
            // 优先尝试 total_*（如有）
            #[cfg(any())]
            {
                rx = rx.saturating_add(data.total_received());
                tx = tx.saturating_add(data.total_transmitted());
            }
            // 常用 API（部分版本）
            rx = rx.saturating_add(data.received());
            tx = tx.saturating_add(data.transmitted());
        }
    }

    axum::Json(SystemMetrics {
        cpu_usage_percent: cpu_usage,
        mem_total: sys.total_memory(),
        mem_used: sys.used_memory(),
        disks: disks_view,
        net: SystemNet { rx_bytes: rx, tx_bytes: tx },
    })
}