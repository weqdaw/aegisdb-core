use std::{sync::{Arc, atomic::{AtomicBool, Ordering}}, collections::VecDeque, time::Instant, path::{Path, PathBuf}};
use tokio::{sync::Mutex, task::JoinHandle, time::{sleep, Duration}};
use serde::{Serialize, Deserialize};
use axum::{Router, routing::{get, post}, Json, extract::State};
use axum::http::StatusCode;
use aegisdb::server::grpc::tinykvpb::tiny_kv_client::TinyKvClient;
use aegisdb::server::grpc::kvrpcpb::{RawPutRequest, RawGetRequest, RawDeleteRequest, RawScanRequest, Context};
use tokio::net::TcpListener;
use axum::http::{Method, HeaderValue, header::{ACCEPT, CONTENT_TYPE}};
use tower_http::cors::CorsLayer;
use rand::Rng;
use clap::Parser;
use rocksdb::{Options, DB, IteratorMode};
use redis::AsyncCommands;

// ---------------------- 金融领域的示例数据结构与生成 ----------------------
#[derive(Serialize, Clone)]
struct Txn {
    txn_id: String,
    account_id: String,
    symbol: String,
    side: String,
    qty: u32,
    price: f64,
    currency: String,
    venue: String,
    ts: i64,
}

#[derive(Serialize, Clone)]
struct Account {
    account_id: String,
    holder: String,
    segment: String,
    balance: f64,
    currency: String,
    risk_level: String,
    opened_at: i64,
}

#[derive(Serialize, Clone)]
struct Quote {
    symbol: String,
    bid: f64,
    ask: f64,
    bid_size: u32,
    ask_size: u32,
    ts: i64,
}

enum FinanceKind { Txn, Account, Quote }
impl FinanceKind {
    fn parse(s: &str) -> Self {
        match s {
            "account" => FinanceKind::Account,
            "quote" => FinanceKind::Quote,
            _ => FinanceKind::Txn,
        }
    }
}

fn pick<'a>(rng: &mut impl Rng, arr: &'a [&'a str]) -> &'a str {
    arr[rng.gen_range(0..arr.len())]
}

fn gen_finance(kind: &FinanceKind, i: u32, prefix: &str, rng: &mut impl Rng) -> (String, Vec<u8>) {
    match kind {
        FinanceKind::Txn => {
            let symbols = ["AAPL","MSFT","TSLA","BABA","NVDA","JPM","V"];
            let venues  = ["NASDAQ","NYSE","ARCA"];
            let sides   = ["BUY","SELL"];
            let ccy     = ["USD","HKD","CNY"];
            let txn_id = format!("T{:08}", i);
            let account_id = format!("A{:06}", rng.gen_range(100000..999999));
            let symbol = pick(rng, &symbols);
            let side = pick(rng, &sides);
            let qty = rng.gen_range(10..1000);
            let price = (rng.gen_range(10_00..2_000_00) as f64) / 100.0;
            let currency = pick(rng, &ccy);
            let venue = pick(rng, &venues);
            let ts = chrono::Utc::now().timestamp_millis();
            let key = format!("{}txn:{}", prefix, txn_id);
            let val = serde_json::to_vec(&Txn{
                txn_id,
                account_id,
                symbol: symbol.to_string(),
                side: side.to_string(),
                qty,
                price,
                currency: currency.to_string(),
                venue: venue.to_string(),
                ts
            }).unwrap();
            (key, val)
        }
        FinanceKind::Account => {
            let seg  = ["retail","vip","insto"];
            let risk = ["low","medium","high"];
            let ccy  = ["USD","HKD","CNY"];
            let account_id = format!("A{:08}", i);
            let holder = format!("User-{:05}", rng.gen_range(1..50000));
            let segment = pick(rng, &seg);
            let balance = (rng.gen_range(0..2_000_000) as f64) / 10.0;
            let currency = pick(rng, &ccy);
            let risk_level = pick(rng, &risk);
            let opened_at = chrono::Utc::now().timestamp() - rng.gen_range(10_000..2_000_000);
            let key = format!("{}acct:{}", prefix, account_id);
            let val = serde_json::to_vec(&Account{
                account_id,
                holder,
                segment: segment.to_string(),
                balance,
                currency: currency.to_string(),
                risk_level: risk_level.to_string(),
                opened_at
            }).unwrap();
            (key, val)
        }
        FinanceKind::Quote => {
            let symbols = ["AAPL","MSFT","TSLA","BABA","NVDA","JPM","V"];
            let symbol = pick(rng, &symbols).to_string();
            // 让报价围绕一个中间价波动
            let mid = (rng.gen_range(50_00..1_500_00) as f64) / 100.0;
            let spread = ((rng.gen_range(1..50)) as f64) / 100.0;
            let bid = (mid - spread/2.0).max(0.01);
            let ask = mid + spread/2.0;
            let bid_size = rng.gen_range(100..5000);
            let ask_size = rng.gen_range(100..5000);
            let ts = chrono::Utc::now().timestamp_millis();
            let key = format!("{}quote:{}", prefix, symbol);
            let val = serde_json::to_vec(&Quote{ symbol, bid, ask, bid_size, ask_size, ts }).unwrap();
            (key, val)
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "ingest_http")]
#[command(about = "AegisDB ingest helper with tier-aware inspection")]
struct Args {
    /// Address to bind the HTTP server on
    #[arg(long, default_value = "127.0.0.1:8088")]
    listen_addr: String,
    /// TinyKV gRPC endpoint (TinyKvClient)
    #[arg(long, default_value = "http://127.0.0.1:20160")]
    kv_endpoint: String,
    /// Root db_path used by TieredStorage (determines default hot/warm/cold directories)
    #[arg(long, default_value = "./aegisdb-data")]
    db_path: PathBuf,
    /// Override path for hot tier (falls back to db_path/hot)
    #[arg(long)]
    hot_path: Option<PathBuf>,
    /// Override path for warm tier (falls back to db_path/warm)
    #[arg(long)]
    warm_path: Option<PathBuf>,
    /// Override path for cold tier (falls back to db_path/cold)
    #[arg(long)]
    cold_path: Option<PathBuf>,
}

#[derive(Clone)]
struct AppState {
    server_addr: String,
    running: Arc<AtomicBool>,
    events: Arc<Mutex<VecDeque<Event>>>,
    metrics: Arc<Mutex<Metrics>>,
    worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    inspector: Arc<TierInspector>,
    migration_progress: Arc<Mutex<MigrationProgress>>,  // 新增
}

#[derive(Serialize, Clone, Default)]
struct MigrationProgress {
    total: u64,
    migrated: u64,
    failed: u64,
    status: String,  // "idle", "running", "completed", "error"
    error: Option<String>,
}

impl AppState {
    fn new(server_addr: String, inspector: TierInspector) -> Self {
        Self {
            server_addr,
            running: Arc::new(AtomicBool::new(false)),
            events: Arc::new(Mutex::new(VecDeque::new())),
            metrics: Arc::new(Mutex::new(Metrics::default())),
            worker: Arc::new(Mutex::new(None)),
            inspector: Arc::new(inspector),
            migration_progress: Arc::new(Mutex::new(MigrationProgress::default())),  // 新增
        }
    }
}

struct TierInspector {
    hot: PathBuf,
    warm: PathBuf,
    cold: PathBuf,
}

impl TierInspector {
    fn new(root: PathBuf, hot: Option<PathBuf>, warm: Option<PathBuf>, cold: Option<PathBuf>) -> Self {
        let hot_path = hot.unwrap_or_else(|| root.join("hot"));
        let warm_path = warm.unwrap_or_else(|| root.join("warm"));
        let cold_path = cold.unwrap_or_else(|| root.join("cold"));
        Self { hot: hot_path, warm: warm_path, cold: cold_path }
    }

    fn collect(&self) -> anyhow::Result<TierSnapshot> {
        Ok(TierSnapshot {
            hot: Self::read_tier(&self.hot)?,
            warm: Self::read_tier(&self.warm)?,
            cold: Self::read_tier(&self.cold)?,
        })
    }

    fn read_tier(path: &Path) -> anyhow::Result<Vec<TierEntry>> {
        if !path.exists() {
            return Ok(Vec::new());
        }

        let mut opts = Options::default();
        opts.create_if_missing(false);

        let db = DB::open_for_read_only(&opts, path, false)?;
        let mut rows = Vec::new();
        for item in db.iterator(IteratorMode::Start) {
            let (key, value) = item?;
            rows.push(TierEntry::from_raw(&key, &value));
        }
        Ok(rows)
    }
}

#[derive(Serialize, Clone)]
struct TierEntry {
    cf: String,
    key: String,
    value: String,
}

impl TierEntry {
    fn from_raw(raw_key: &[u8], raw_value: &[u8]) -> Self {
        let (cf, key) = match raw_key.iter().position(|b| *b == b'_') {
            Some(idx) => (
                String::from_utf8_lossy(&raw_key[..idx]).to_string(),
                String::from_utf8_lossy(&raw_key[idx + 1..]).to_string(),
            ),
            None => ("default".to_string(), String::from_utf8_lossy(raw_key).to_string()),
        };
        Self {
            cf,
            key,
            value: String::from_utf8_lossy(raw_value).to_string(),
        }
    }
}

#[derive(Serialize, Clone, Default)]
struct TierSnapshot {
    hot: Vec<TierEntry>,
    warm: Vec<TierEntry>,
    cold: Vec<TierEntry>,
}

#[derive(Serialize, Clone)]
struct Metrics {
    put: u64,
    get: u64,
    del: u64,
    err: u64,
    // 额外业务维度统计（不影响现有前端）
    put_txn: u64,
    put_account: u64,
    put_quote: u64,
}
impl Default for Metrics {
    fn default() -> Self { Self { put:0, get:0, del:0, err:0, put_txn:0, put_account:0, put_quote:0 } }
}

#[derive(Serialize, Clone)]
struct Event {
    ts: u128,
    op: &'static str,
    key: String,
    ok: bool,
    ms: u128,
    err: Option<String>,
    // 可选的业务类型和样例内容，便于前端调试查看
    kind: Option<&'static str>,
    sample: Option<String>,
}

#[derive(Deserialize)]
struct StartBody {
    count: Option<u32>,       // 默认 500
    prefix: Option<String>,   // 默认 "demo:"
    get_ratio: Option<f32>,   // 默认 0.1
    del_ratio: Option<f32>,   // 默认 0.05
    kind: Option<String>,     // txn/account/quote，默认 txn
}

#[derive(Deserialize)]
struct KvOperation {
    op: String,      // "put", "get", "delete"
    cf: Option<String>,  // 默认 "default"
    key: String,
    value: Option<String>,  // PUT 操作需要
}

#[derive(Serialize)]
struct KvResponse {
    success: bool,
    message: String,
    data: Option<String>,  // GET 操作返回的值
}

#[derive(Deserialize)]
struct MigrationRequest {
    redis_url: Option<String>,  // 默认 "redis://127.0.0.1:6379"
    prefix: Option<String>,     // 可选：只迁移特定前缀的key
}

#[derive(Serialize)]
struct MigrationResponse {
    success: bool,
    message: String,
    progress: MigrationProgress,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let inspector = TierInspector::new(
        args.db_path.clone(),
        args.hot_path.clone(),
        args.warm_path.clone(),
        args.cold_path.clone(),
    );
    let state = AppState::new(args.kv_endpoint.clone(), inspector);

    let app = Router::new()
    .route("/api/ingest/start", post(start))
    .route("/api/ingest/stop", post(stop))
    .route("/api/biz/metrics", get(get_metrics))
    .route("/api/biz/events", get(get_events))
    .route("/api/storage/tiers", get(get_tier_snapshot))
    .route("/api/kv/execute", post(execute_kv_operation))
    .route("/api/migration/start", post(start_migration))      // 新增
    .route("/api/migration/progress", get(get_migration_progress))  // 新增
    .with_state(state)
    .layer({
        // 允许来自前端开发端口的跨域
        let mut origins: Vec<HeaderValue> = Vec::new();
        origins.push(HeaderValue::from_static("http://localhost:5173"));
        origins.push(HeaderValue::from_static("http://127.0.0.1:5173"));

        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
            .allow_headers([ACCEPT, CONTENT_TYPE])
            .allow_credentials(false)
    });

        println!("Ingest HTTP listening on http://{}", args.listen_addr);
        let listener = TcpListener::bind(&args.listen_addr).await?;
        axum::serve(listener, app).await?;
    Ok(())
}

async fn start(State(state): State<AppState>, Json(body): Json<StartBody>) -> Json<&'static str> {
    if state.running.swap(true, Ordering::SeqCst) {
        return Json("already running");
    }
    // 清空历史指标与事件
    {
        let mut m = state.metrics.lock().await;
        *m = Metrics::default();
        let mut q = state.events.lock().await;
        q.clear();
    }
    let count = body.count.unwrap_or(500);
    let prefix = body.prefix.unwrap_or_else(|| "demo:".to_string());
    let get_ratio = body.get_ratio.unwrap_or(0.10f32);
    let del_ratio = body.del_ratio.unwrap_or(0.05f32);
    let kind = FinanceKind::parse(body.kind.as_deref().unwrap_or("txn"));

    let st = state.clone();
    let server_addr = state.server_addr.clone();
    let handle = tokio::spawn(async move {
        let mut client = match TinyKvClient::connect(server_addr.clone()).await {
            Ok(c) => c,
            Err(e) => {
                record_err(&st, "connect", "", &e.to_string()).await;
                st.running.store(false, Ordering::SeqCst);
                return;
            }
        };

        let cf = "default".to_string();
        let ctx = Some(Context{ region_id: 1, region_epoch: None, peer: None, term: 0 });

        for i in 0..count {
            if !st.running.load(Ordering::SeqCst) { break; }
            // 生成符合金融场景的 key/value
            let (key, val) = {
                let mut rng = rand::thread_rng();
                gen_finance(&kind, i, &prefix, &mut rng)
            };
            let kind_tag: &'static str = match kind {
                FinanceKind::Txn => "txn",
                FinanceKind::Account => "account",
                FinanceKind::Quote => "quote",
            };
            let sample = match std::str::from_utf8(&val) {
                Ok(s) => Some(truncate_for_event(s, 200).to_string()),
                Err(_) => None,
            };

            // Put
            let t0 = Instant::now();
            let res = client.raw_put(RawPutRequest{ context: ctx.clone(), key: key.as_bytes().to_vec(), value: val, cf: cf.clone() }).await;
            let ok = res.is_ok();
            record(&st, "put", &key, ok, t0.elapsed().as_millis(), res.err(), Some(kind_tag), sample).await;

            // 随机做 get/del（按比率）
            if rand_bool(get_ratio) {
                let t1 = Instant::now();
                let gr = client.raw_get(RawGetRequest{ context: ctx.clone(), key: key.as_bytes().to_vec(), cf: cf.clone() }).await;
                let ok = gr.is_ok();
                record(&st, "get", &key, ok, t1.elapsed().as_millis(), gr.err(), Some(kind_tag), None).await;
            }
            if rand_bool(del_ratio) {
                let t2 = Instant::now();
                let dr = client.raw_delete(RawDeleteRequest{ context: ctx.clone(), key: key.as_bytes().to_vec(), cf: cf.clone() }).await;
                let ok = dr.is_ok();
                record(&st, "del", &key, ok, t2.elapsed().as_millis(), dr.err(), Some(kind_tag), None).await;
            }

            // 轻微节流，避免瞬时把存储打满
            sleep(Duration::from_millis(2)).await;
        }

        // 收尾：做一次 scan 样例（不强制）
        let _ = client.raw_scan(RawScanRequest{ context: ctx.clone(), start_key: prefix.as_bytes().to_vec(), limit: 10, cf: cf.clone() }).await;

        st.running.store(false, Ordering::SeqCst);
    });

    *state.worker.lock().await = Some(handle);
    Json("started")
}

async fn stop(State(state): State<AppState>) -> Json<&'static str> {
    state.running.store(false, Ordering::SeqCst);
    if let Some(h) = state.worker.lock().await.take() {
        h.abort();
    }
    Json("stopped")
}

async fn get_metrics(State(state): State<AppState>) -> Json<Metrics> {
    Json(state.metrics.lock().await.clone())
}

async fn get_events(State(state): State<AppState>) -> Json<Vec<Event>> {
    let q = state.events.lock().await;
    Json(q.iter().cloned().collect())
}

async fn get_tier_snapshot(State(state): State<AppState>) -> Result<Json<TierSnapshot>, StatusCode> {
    let inspector = state.inspector.clone();
    tokio::task::spawn_blocking(move || inspector.collect())
        .await
        .map_err(|err| {
            eprintln!("tier snapshot task join error: {err}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .map(Json)
        .map_err(|err| {
            eprintln!("tier snapshot read error: {err}");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

async fn execute_kv_operation(
    State(state): State<AppState>,
    Json(body): Json<KvOperation>,
) -> Result<Json<KvResponse>, StatusCode> {
    let cf = body.cf.unwrap_or_else(|| "default".to_string());
    let ctx = Some(Context {
        region_id: 1,
        region_epoch: None,
        peer: None,
        term: 0,
    });

    let mut client = match TinyKvClient::connect(state.server_addr.clone()).await {
        Ok(c) => c,
        Err(e) => {
            return Ok(Json(KvResponse {
                success: false,
                message: format!("连接失败: {}", e),
                data: None,
            }));
        }
    };

    let t0 = Instant::now();
    let result = match body.op.as_str() {
        "put" => {
            if let Some(value) = body.value {
                let val_bytes = value.as_bytes().to_vec();
                let res = client
                    .raw_put(RawPutRequest {
                        context: ctx.clone(),
                        key: body.key.as_bytes().to_vec(),
                        value: val_bytes,
                        cf: cf.clone(),
                    })
                    .await;
                if res.is_ok() {
                    record(
                        &state,
                        "put",
                        &body.key,
                        true,
                        t0.elapsed().as_millis(),
                        None,
                        None,
                        Some(truncate_for_event(&value, 200).to_string()),
                    )
                    .await;
                    Ok(KvResponse {
                        success: true,
                        message: "PUT 操作成功".to_string(),
                        data: None,
                    })
                } else {
                    let err = res.err().unwrap();
                    record_err(&state, "put", &body.key, &err.to_string()).await;
                    Ok(KvResponse {
                        success: false,
                        message: format!("PUT 操作失败: {}", err),
                        data: None,
                    })
                }
            } else {
                Ok(KvResponse {
                    success: false,
                    message: "PUT 操作需要提供 value 参数".to_string(),
                    data: None,
                })
            }
        }
        "get" => {
            let res = client
                .raw_get(RawGetRequest {
                    context: ctx.clone(),
                    key: body.key.as_bytes().to_vec(),
                    cf: cf.clone(),
                })
                .await;
            match res {
                Ok(response) => {
                    let resp = response.into_inner();
                    let value = if resp.not_found {
                        "null".to_string()
                    } else {
                        String::from_utf8_lossy(&resp.value).to_string()
                    };
                    record(
                        &state,
                        "get",
                        &body.key,
                        true,
                        t0.elapsed().as_millis(),
                        None,
                        None,
                        Some(truncate_for_event(&value, 200).to_string()),
                    )
                    .await;
                    Ok(KvResponse {
                        success: true,
                        message: "GET 操作成功".to_string(),
                        data: Some(value),
                    })
                }
                Err(e) => {
                    record_err(&state, "get", &body.key, &e.to_string()).await;
                    Ok(KvResponse {
                        success: false,
                        message: format!("GET 操作失败: {}", e),
                        data: None,
                    })
                }
            }
        }
        "delete" => {
            let res = client
                .raw_delete(RawDeleteRequest {
                    context: ctx.clone(),
                    key: body.key.as_bytes().to_vec(),
                    cf: cf.clone(),
                })
                .await;
            if res.is_ok() {
                record(
                    &state,
                    "del",
                    &body.key,
                    true,
                    t0.elapsed().as_millis(),
                    None,
                    None,
                    None,
                )
                .await;
                Ok(KvResponse {
                    success: true,
                    message: "DELETE 操作成功".to_string(),
                    data: None,
                })
            } else {
                let err = res.err().unwrap();
                record_err(&state, "del", &body.key, &err.to_string()).await;
                Ok(KvResponse {
                    success: false,
                    message: format!("DELETE 操作失败: {}", err),
                    data: None,
                })
            }
        }
        _ => Ok(KvResponse {
            success: false,
            message: format!("不支持的操作类型: {}", body.op),
            data: None,
        }),
    };

    result.map(Json)
}

async fn start_migration(
    State(state): State<AppState>,
    Json(body): Json<MigrationRequest>,
) -> Result<Json<MigrationResponse>, StatusCode> {
    // 检查是否已有迁移任务在运行
    let mut progress = state.migration_progress.lock().await;
    if progress.status == "running" {
        return Ok(Json(MigrationResponse {
            success: false,
            message: "迁移任务已在运行中".to_string(),
            progress: progress.clone(),
        }));
    }

    let redis_url = body.redis_url.unwrap_or_else(|| "redis://127.0.0.1:6379".to_string());
    let prefix = body.prefix.clone();

    // 重置进度
    *progress = MigrationProgress {
        total: 0,
        migrated: 0,
        failed: 0,
        status: "running".to_string(),  // 已经是 String，正确
        error: None,
    };
    let state_clone = state.clone();

    // 启动迁移任务
    tokio::spawn(async move {
        if let Err(e) = migrate_to_redis(&state_clone, &redis_url, prefix.as_deref()).await {
            let mut p = state_clone.migration_progress.lock().await;
            p.status = "error".to_string();
            p.error = Some(e.to_string());
        } else {
            let mut p = state_clone.migration_progress.lock().await;
            p.status = "completed".to_string();
        }
    });

    Ok(Json(MigrationResponse {
        success: true,
        message: "迁移任务已启动".to_string(),
        progress: progress.clone(),
    }))
}

async fn get_migration_progress(
    State(state): State<AppState>,
) -> Json<MigrationProgress> {
    Json(state.migration_progress.lock().await.clone())
}

async fn migrate_to_redis(
    state: &AppState,
    redis_url: &str,
    prefix_filter: Option<&str>,
) -> anyhow::Result<()> {
    // 连接 Redis - 使用 anyhow::Result，不需要转换为 StatusCode
    let client = redis::Client::open(redis_url)
        .map_err(|e| anyhow::anyhow!("Redis 连接失败: {}", e))?;
    let mut conn = client.get_async_connection().await
        .map_err(|e| anyhow::anyhow!("Redis 异步连接失败: {}", e))?;

    // 获取所有层的数据
    let snapshot = state.inspector.collect()?;
    
    // 统计总数
    let total = snapshot.hot.len() + snapshot.warm.len() + snapshot.cold.len();
    {
        let mut progress = state.migration_progress.lock().await;
        progress.total = total as u64;
    }

    // 合并所有层的数据
    let mut all_entries = Vec::new();
    all_entries.extend(snapshot.hot);
    all_entries.extend(snapshot.warm);
    all_entries.extend(snapshot.cold);

    // 如果指定了前缀过滤，进行过滤
    let entries_to_migrate: Vec<_> = if let Some(prefix) = prefix_filter {
        all_entries.into_iter()
            .filter(|e| e.key.starts_with(prefix))
            .collect()
    } else {
        all_entries
    };

    // 更新总数（过滤后）
    {
        let mut progress = state.migration_progress.lock().await;
        progress.total = entries_to_migrate.len() as u64;
    }

    // 迁移数据 - 直接使用 set，不使用事务
    for entry in entries_to_migrate {
        // 构建 Redis key（包含 CF 信息）
        let redis_key = if entry.cf == "default" {
            entry.key.clone()
        } else {
            format!("{}:{}", entry.cf, entry.key)
        };

        // 写入 Redis - 使用 set 方法，value 应该是字符串或字节
        match conn.set::<_, _, ()>(&redis_key, entry.value.as_str()).await {
            Ok(_) => {
                let mut progress = state.migration_progress.lock().await;
                progress.migrated += 1;
            }
            Err(e) => {
                eprintln!("迁移 key {} 失败: {}", redis_key, e);
                let mut progress = state.migration_progress.lock().await;
                progress.failed += 1;
            }
        }
    }

    Ok(())
}

fn rand_bool(p: f32) -> bool {
    use rand::{Rng, thread_rng};
    let mut rng = thread_rng();
    rng.gen::<f32>() < p
}

fn truncate_for_event(s: &str, max_len: usize) -> &str {
    if s.len() <= max_len { s } else { &s[..max_len] }
}

async fn record(state: &AppState, op: &'static str, key: &str, ok: bool, ms: u128, err: Option<tonic::Status>, kind: Option<&'static str>, sample: Option<String>) {
    // 事件
    let mut q = state.events.lock().await;
    q.push_front(Event{
        ts: chrono::Utc::now().timestamp_millis() as u128,
        op, key: key.to_string(), ok, ms,
        err: err.as_ref().map(|e| e.to_string()),
        kind,
        sample,
    });
    while q.len() > 200 { q.pop_back(); }

    // 指标
    let mut m = state.metrics.lock().await;
    match op {
        "put" => m.put += 1,
        "get" => m.get += 1,
        "del" => m.del += 1,
        _ => {}
    }
    if op == "put" {
        match kind {
            Some("txn") => m.put_txn += 1,
            Some("account") => m.put_account += 1,
            Some("quote") => m.put_quote += 1,
            _ => {}
        }
    }
    if !ok { m.err += 1; }
}

async fn record_err(state: &AppState, op: &'static str, key: &str, msg: &str) {
    let mut q = state.events.lock().await;
    q.push_front(Event{
        ts: chrono::Utc::now().timestamp_millis() as u128,
        op, key: key.to_string(), ok: false, ms: 0, err: Some(msg.to_string()),
        kind: None,
        sample: None,
    });
    while q.len() > 200 { q.pop_back(); }
    let mut m = state.metrics.lock().await;
    m.err += 1;
}