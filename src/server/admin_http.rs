// aegisdb/src/server/admin_http.rs
use axum::{routing::get, Router};
use axum::extract::State;
use serde::Serialize;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

use crate::scheduler::cluster::{BasicCluster, StoreInfo, RegionInfo};
use crate::scheduler::coordinator::Coordinator;
use crate::raftstore::store_balancer::StoreLoad;

#[derive(Clone)]
pub struct AppState {
    pub cluster: Arc<BasicCluster>,
    pub coordinator: Arc<Coordinator>,
    pub get_cluster_id: Arc<dyn Fn() -> u64 + Send + Sync>,
    pub store_loads: Arc<parking_lot::RwLock<Vec<StoreLoad>>>,
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

fn to_store_view(s: &StoreInfo) -> ApiStore {
    ApiStore {
        id: s.id(),
        address: s.address().to_string(),
        state: format!("{:?}", s.state()),
        region_count: s.region_count(),
        leader_count: s.leader_count(),
        region_size: s.region_size(),
        leader_size: s.leader_size(),
    }
}

fn to_region_view(r: &RegionInfo) -> ApiRegion {
    let start = String::from_utf8_lossy(r.start_key()).to_string();
    let end = String::from_utf8_lossy(r.end_key()).to_string();
    ApiRegion {
        id: r.id(),
        start_key: start,
        end_key: end,
        leader_store_id: r.get_leader_store_id(),
        approximate_size: r.approximate_size(),
        peer_store_ids: r.peers().iter().map(|p| p.store_id).collect(),
    }
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
        .route("/api/system", get(get_system))
        .with_state(state)
        .layer(cors);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn get_cluster_summary(State(state): State<AppState>) -> axum::Json<ClusterSummary> {
    let cluster_id = (state.get_cluster_id)();
    let stores = state.cluster.get_stores();
    let regions = state.cluster.get_regions();

    let store_count = stores.len();
    let region_count = regions.len();
    let leader_count: usize = stores.iter().map(|s| s.leader_count()).sum();
    let total_region_size: u64 = regions.iter().map(|r| r.approximate_size()).sum();

    axum::Json(ClusterSummary {
        cluster_id,
        store_count,
        region_count,
        leader_count,
        total_region_size,
    })
}

async fn list_stores(State(state): State<AppState>) -> axum::Json<Vec<ApiStore>> {
    let stores = state.cluster.get_stores();
    axum::Json(stores.iter().map(to_store_view).collect())
}

async fn list_regions(State(state): State<AppState>) -> axum::Json<Vec<ApiRegion>> {
    let regions = state.cluster.get_regions();
    axum::Json(regions.iter().map(to_region_view).collect())
}

async fn list_operators(State(state): State<AppState>) -> axum::Json<OperatorSummary> {
    // 你可以在 Coordinator/OperatorController 内部补充对外统计函数；此处先用简化视图
    let oc = state.coordinator.op_controller();
    let running = oc.len_running();
    let pending = oc.len_pending();
    let finished = oc.len_finished();
    axum::Json(OperatorSummary { running, pending, finished })
}

async fn list_store_loads(State(state): State<AppState>) -> axum::Json<Vec<StoreLoadView>> {
    let loads = state.store_loads.read();
    axum::Json(loads.iter().map(|l| StoreLoadView {
        store_id: l.store_id,
        region_count: l.region_count,
        leader_count: l.leader_count,
        region_size: l.region_size,
        leader_size: l.leader_size,
    }).collect())
}

#[derive(Serialize)]
struct SystemDisk { name: String, total: u64, used: u64 }

#[derive(Serialize)]
struct SystemNet { rx_bytes: u64, tx_bytes: u64 }

#[derive(Serialize)]
struct SystemMetrics {
    cpu_usage_percent: f32,
    mem_total: u64,
    mem_used: u64,
    disks: Vec<SystemDisk>,
    net: SystemNet,
}

async fn get_system() -> axum::Json<SystemMetrics> {
    use sysinfo::System;

    let mut sys = System::new_all();
    sys.refresh_all();

    // CPU usage (global)
    let cpu = sys.global_cpu_info().cpu_usage();

    // Memory
    let mem_total = sys.total_memory();
    let mem_used = sys.used_memory();

    // Disks: keep empty if disk enumeration is unavailable
    let disks: Vec<SystemDisk> = Vec::new();

    // Network totals (cumulative)
    let rx: u64 = 0;
    let tx: u64 = 0;

    axum::Json(SystemMetrics {
        cpu_usage_percent: cpu,
        mem_total,
        mem_used,
        disks,
        net: SystemNet { rx_bytes: rx, tx_bytes: tx },
    })
}

// 需要在 OperatorController 上补这三个方法，若已有可直接使用；若没有，请在其实现中返回内部队列长度。
pub trait OperatorControllerExt {
    fn len_running(&self) -> usize;
    fn len_pending(&self) -> usize;
    fn len_finished(&self) -> usize;
}

impl OperatorControllerExt for crate::scheduler::operator_controller::OperatorController {
    fn len_running(&self) -> usize { self.running_len() }
    fn len_pending(&self) -> usize { self.pending_len() }
    fn len_finished(&self) -> usize { self.finished_len() }
}