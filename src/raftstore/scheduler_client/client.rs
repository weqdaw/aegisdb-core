use crate::proto::metapb::{Region, Peer, Store};
use crate::proto::schedulerpb::*;
use anyhow::{Result, anyhow};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tokio::time::Duration;
use log::{info, warn, error};

const SCHEDULER_TIMEOUT: Duration = Duration::from_secs(1);
const RETRY_INTERVAL: Duration = Duration::from_secs(1);
const MAX_RETRY_COUNT: usize = 10;
const MAX_INIT_CLUSTER_RETRIES: usize = 100;

/// 调度器客户端接口
#[async_trait::async_trait]
pub trait SchedulerClient: Send + Sync {
    /// 获取集群 ID
    fn get_cluster_id(&self) -> u64;
    
    /// 创建请求头
    fn request_header(&self) -> RequestHeader;
    
    /// 分配 ID
    async fn alloc_id(&self) -> Result<u64>;
    
    /// 初始化集群
    async fn bootstrap(&self, store: &Store) -> Result<BootstrapResponse>;
    
    /// 检查集群是否已初始化
    async fn is_bootstrapped(&self) -> Result<bool>;
    
    /// 注册 Store
    async fn put_store(&self, store: &Store) -> Result<()>;
    
    /// 获取 Store
    async fn get_store(&self, store_id: u64) -> Result<Store>;
    
    /// 根据 key 获取 Region
    async fn get_region(&self, key: &[u8]) -> Result<(Region, Option<Peer>)>;
    
    /// 根据 Region ID 获取 Region
    async fn get_region_by_id(&self, region_id: u64) -> Result<(Region, Option<Peer>)>;
    
    /// 请求 Split
    async fn ask_split(&self, region: &Region) -> Result<AskSplitResponse>;
    
    /// Store 心跳
    async fn store_heartbeat(&self, stats: &StoreStats) -> Result<()>;
    
    /// Region 心跳（异步发送）
    fn region_heartbeat(&self, request: RegionHeartbeatRequest) -> Result<()>;
    
    /// 设置 Region 心跳响应处理器
    fn set_region_heartbeat_response_handler(
        &self,
        store_id: u64,
        handler: Box<dyn Fn(RegionHeartbeatResponse) + Send + Sync>,
    );
    
    /// 关闭客户端
    async fn close(&self);
}

/// 调度器客户端实现
pub struct Client {
    urls: Vec<String>,
    cluster_id: Arc<AtomicU64>,  // 改为 AtomicU64
    tag: String,
    
    // 心跳相关
    region_heartbeat_tx: mpsc::UnboundedSender<RegionHeartbeatRequest>,
    heartbeat_handler: Arc<RwLock<Option<Box<dyn Fn(RegionHeartbeatResponse) + Send + Sync>>>>,
    
    // 关闭信号
    shutdown_tx: mpsc::UnboundedSender<()>,
    
    // 模拟响应通道（用于测试）
    response_tx: mpsc::UnboundedSender<RegionHeartbeatResponse>,
}

impl Client {
    /// 创建新的调度器客户端
    pub async fn new(urls: Vec<String>, tag: String) -> Result<Self> {
        let urls: Vec<String> = urls.into_iter()
            .map(|url| {
                if url.contains("://") {
                    url
                } else {
                    format!("http://{}", url)
                }
            })
            .collect();
        
        info!("[{}][scheduler] create scheduler client with endpoints {:?}", tag, urls);
        
        let (region_heartbeat_tx, region_heartbeat_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = mpsc::unbounded_channel();
        let (response_tx, mut response_rx) = mpsc::unbounded_channel();
        
        let cluster_id = Arc::new(AtomicU64::new(0));
        let heartbeat_handler: Arc<RwLock<Option<Box<dyn Fn(RegionHeartbeatResponse) + Send + Sync>>>> = 
            Arc::new(RwLock::new(None));
        
        // 初始化集群 ID
        let cluster_id_clone = cluster_id.clone();
        let urls_clone = urls.clone();
        let tag_clone = tag.clone();
        tokio::spawn(async move {
            for i in 0..MAX_RETRY_COUNT {
                if let Ok(members) = Self::get_members_internal(&urls_clone).await {
                    let id = members.header.as_ref()
                        .map(|h| h.cluster_id)
                        .unwrap_or(0);
                    cluster_id_clone.store(id, Ordering::Relaxed);
                    info!("[{}][scheduler] init cluster id {}", tag_clone, id);
                    break;
                }
                if i < MAX_RETRY_COUNT - 1 {
                    tokio::time::sleep(RETRY_INTERVAL).await;
                }
            }
        });
        
        // 启动心跳流处理
        let heartbeat_handler_clone = heartbeat_handler.clone();
        let tag_clone2 = tag.clone();
        
        let client = Self {
            urls,
            cluster_id: cluster_id.clone(),
            tag: tag.clone(),
            region_heartbeat_tx,
            heartbeat_handler: heartbeat_handler.clone(),
            shutdown_tx,
            response_tx: response_tx.clone(),
        };
        
        // 启动心跳流循环（传递需要的字段，而不是整个 client）
        let cluster_id_for_loop = cluster_id.clone();
        let response_tx_for_loop = response_tx.clone();
        tokio::spawn(async move {
            Self::heartbeat_stream_loop(
                region_heartbeat_rx,
                heartbeat_handler_clone,
                shutdown_rx,
                tag_clone2,
                cluster_id_for_loop,
                response_tx_for_loop,
            ).await;
        });
        
        // 启动响应处理循环
        let handler_for_response = heartbeat_handler.clone();
        tokio::spawn(async move {
            while let Some(resp) = response_rx.recv().await {
                if let Ok(handler_guard) = handler_for_response.read() {
                    if let Some(handler) = handler_guard.as_ref() {
                        handler(resp);
                    }
                }
            }
        });
        
        Ok(client)
    }
    
    /// 获取成员信息（内部实现）
    async fn get_members_internal(_urls: &[String]) -> Result<GetMembersResponse> {
        // 模拟实现：返回一个模拟的响应
        // 在实际环境中，这里应该调用真实的 gRPC 服务
        Ok(GetMembersResponse {
            header: Some(ResponseHeader {
                cluster_id: 1,
                error: None,
            }),
            members: vec![Member {
                member_id: 1,
                client_urls: vec!["http://127.0.0.1:2379".to_string()],
                peer_urls: vec!["http://127.0.0.1:2380".to_string()],
            }],
            leader: Some(Member {
                member_id: 1,
                client_urls: vec!["http://127.0.0.1:2379".to_string()],
                peer_urls: vec!["http://127.0.0.1:2380".to_string()],
            }),
        })
    }
    
    /// 心跳流处理循环
    async fn heartbeat_stream_loop(
        mut region_heartbeat_rx: mpsc::UnboundedReceiver<RegionHeartbeatRequest>,
        _heartbeat_handler: Arc<RwLock<Option<Box<dyn Fn(RegionHeartbeatResponse) + Send + Sync>>>>,
        mut shutdown_rx: mpsc::UnboundedReceiver<()>,
        tag: String,
        cluster_id: Arc<AtomicU64>,
        response_tx: mpsc::UnboundedSender<RegionHeartbeatResponse>,
    ) {
        info!("[{}][scheduler] heartbeat stream loop started", tag);
        
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("[{}][scheduler] heartbeat stream loop shutdown", tag);
                    break;
                }
                
                request = region_heartbeat_rx.recv() => {
                    match request {
                        Some(req) => {
                            // 处理心跳请求
                            Self::process_heartbeat_request(
                                &cluster_id,
                                &response_tx,
                                req,
                                &tag,
                            ).await;
                        }
                        None => {
                            warn!("[{}][scheduler] heartbeat channel closed", tag);
                            break;
                        }
                    }
                }
                
                // 定期检查（用于保持循环活跃）
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    // 定期检查，保持循环活跃
                    info!("[{}][scheduler] heartbeat stream loop still alive", tag);
                }
            }
        }
    }
    
    /// 处理心跳请求（模拟实现）
    async fn process_heartbeat_request(
        cluster_id: &Arc<AtomicU64>,
        response_tx: &mpsc::UnboundedSender<RegionHeartbeatResponse>,
        request: RegionHeartbeatRequest,
        tag: &str,
    ) {
        // 模拟处理心跳请求
        if let Some(region) = &request.region {
            info!("[{}][scheduler] processing heartbeat for region {}", tag, region.id);
            
            // 模拟生成响应
            let response = RegionHeartbeatResponse {
                header: Some(ResponseHeader {
                    cluster_id: cluster_id.load(Ordering::Relaxed),
                    error: None,
                }),
                change_peer: None,
                transfer_leader: None,
                region_id: region.id,
                region_epoch: Some(region.region_epoch.clone()),
                target_peer: None,
            };
            
            // 发送响应到响应通道
            if let Err(e) = response_tx.send(response) {
                error!("[{}][scheduler] failed to send heartbeat response: {:?}", tag, e);
            }
        }
    }
    
    /// 执行请求（带重试）
    async fn do_request<F, T>(&self, f: F) -> Result<T>
    where
        F: Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + Send>>,
    {
        let mut last_err = None;
        for _ in 0..MAX_RETRY_COUNT {
            match tokio::time::timeout(SCHEDULER_TIMEOUT, f()).await {
                Ok(Ok(result)) => return Ok(result),
                Ok(Err(e)) => {
                    last_err = Some(e);
                    tokio::time::sleep(RETRY_INTERVAL).await;
                }
                Err(_) => {
                    last_err = Some(anyhow!("timeout"));
                    tokio::time::sleep(RETRY_INTERVAL).await;
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("failed too many times")))
    }
    
}

#[async_trait::async_trait]
impl SchedulerClient for Client {
    fn get_cluster_id(&self) -> u64 {
        self.cluster_id.load(Ordering::Relaxed)  // 改为 load
    }
    
    fn request_header(&self) -> RequestHeader {
        RequestHeader {
            cluster_id: self.cluster_id.load(Ordering::Relaxed),  // 改为 load
        }
    }
    
    async fn alloc_id(&self) -> Result<u64> {
        // TODO: 实现实际的 gRPC 调用
        Ok(rand::random::<u64>())
    }
    
    async fn bootstrap(&self, _store: &Store) -> Result<BootstrapResponse> {
        // TODO: 实现实际的 gRPC 调用
        Ok(BootstrapResponse {
            header: Some(ResponseHeader {
                cluster_id: self.get_cluster_id(),
                error: None,
            }),
        })
    }
    
    async fn is_bootstrapped(&self) -> Result<bool> {
        // TODO: 实现实际的 gRPC 调用
        Ok(false)
    }
    
    async fn put_store(&self, _store: &Store) -> Result<()> {
        // TODO: 实现实际的 gRPC 调用
        Ok(())
    }
    
    async fn get_store(&self, _store_id: u64) -> Result<Store> {
        // TODO: 实现实际的 gRPC 调用
        Err(anyhow!("not implemented"))
    }
    
    async fn get_region(&self, _key: &[u8]) -> Result<(Region, Option<Peer>)> {
        // TODO: 实现实际的 gRPC 调用
        Err(anyhow!("not implemented"))
    }
    
    async fn get_region_by_id(&self, _region_id: u64) -> Result<(Region, Option<Peer>)> {
        // TODO: 实现实际的 gRPC 调用
        Err(anyhow!("not implemented"))
    }
    
    async fn ask_split(&self, _region: &Region) -> Result<AskSplitResponse> {
        // TODO: 实现实际的 gRPC 调用
        Ok(AskSplitResponse {
            header: Some(ResponseHeader {
                cluster_id: self.get_cluster_id(),
                error: None,
            }),
            new_region_id: rand::random::<u64>(),
            new_peer_ids: vec![rand::random::<u64>()],
        })
    }
    
    async fn store_heartbeat(&self, _stats: &StoreStats) -> Result<()> {
        // TODO: 实现实际的 gRPC 调用
        Ok(())
    }
    
    fn region_heartbeat(&self, request: RegionHeartbeatRequest) -> Result<()> {
        self.region_heartbeat_tx.send(request)
            .map_err(|e| anyhow!("failed to send region heartbeat: {:?}", e))
    }
    
    fn set_region_heartbeat_response_handler(
        &self,
        _store_id: u64,
        handler: Box<dyn Fn(RegionHeartbeatResponse) + Send + Sync>,
    ) {
        if let Ok(mut handler_guard) = self.heartbeat_handler.write() {
            *handler_guard = Some(handler);
        }
    }
    
    async fn close(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("urls", &self.urls)
            .field("cluster_id", &self.cluster_id.load(Ordering::Relaxed))
            .field("tag", &self.tag)
            .finish_non_exhaustive()
    }
}