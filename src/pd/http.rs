use crate::pd::state::PdState;
use crate::raftstore::store_balancer::StoreLoad;
use axum::{routing::get, Router};
use axum::extract::State;
use serde::Serialize;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

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