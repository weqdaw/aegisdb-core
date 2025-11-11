use crate::proto::metapb::{Peer, Region, Store};
use crate::proto::schedulerpb::{
    self,
    pd_client::PdClient,
    AskSplitRequest,
    AskSplitResponse,
    AllocIdRequest,
    BootstrapRequest,
    BootstrapResponse,
    GetMembersRequest,
    GetMembersResponse,
    GetRegionByIdRequest,
    GetRegionRequest,
    GetRegionResponse,
    GetStoreRequest,
    GetStoreResponse,
    IsBootstrappedRequest,
    PutStoreRequest,
    RegionHeartbeatRequest,
    RegionHeartbeatResponse,
    RequestHeader,
    StoreHeartbeatRequest,
    StoreHeartbeatResponse,
    StoreStats,
};
use anyhow::{anyhow, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

const HEARTBEAT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[async_trait::async_trait]
pub trait SchedulerClient: Send + Sync {
    fn get_cluster_id(&self) -> u64;
    fn request_header(&self) -> RequestHeader;
    async fn alloc_id(&self) -> Result<u64>;
    async fn bootstrap(&self, store: &Store) -> Result<BootstrapResponse>;
    async fn is_bootstrapped(&self) -> Result<bool>;
    async fn put_store(&self, store: &Store) -> Result<()>;
    async fn get_store(&self, store_id: u64) -> Result<Store>;
    async fn get_region(&self, key: &[u8]) -> Result<(Region, Option<Peer>)>;
    async fn get_region_by_id(&self, region_id: u64) -> Result<(Region, Option<Peer>)>;
    async fn ask_split(&self, region: &Region) -> Result<AskSplitResponse>;
    async fn store_heartbeat(&self, stats: &StoreStats) -> Result<()>;
    fn region_heartbeat(&self, request: RegionHeartbeatRequest) -> Result<()>;
    fn set_region_heartbeat_response_handler(
        &self,
        handler: Box<dyn Fn(RegionHeartbeatResponse) + Send + Sync>,
    );
    async fn close(&self);
}

#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    tag: String,
    channel: Channel,
    cluster_id: AtomicU64,
    store_id: u64,
    region_tx: mpsc::UnboundedSender<RegionHeartbeatRequest>,
    response_handler: Arc<RwLock<Option<Box<dyn Fn(RegionHeartbeatResponse) + Send + Sync>>>>,
    shutdown_tx: mpsc::UnboundedSender<()>,
}

impl Client {
    pub async fn connect(
        endpoints: Vec<String>,
        store_id: u64,
        tag: String,
    ) -> Result<Self> {
        let normalized: Vec<String> = endpoints
            .into_iter()
            .map(|url| {
                if url.starts_with("http://") || url.starts_with("https://") {
                    url
                } else {
                    format!("http://{}", url)
                }
            })
            .collect();

        let endpoint = normalized
            .first()
            .ok_or_else(|| anyhow!("empty pd endpoints"))?
            .clone();

        let channel = Endpoint::from_shared(endpoint.clone())?
            .connect()
            .await?;

        let (region_tx, region_rx) = mpsc::unbounded_channel();
        let region_stream = UnboundedReceiverStream::new(region_rx);

        let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel::<()>();
        let response_handler: Arc<RwLock<Option<Box<dyn Fn(RegionHeartbeatResponse) + Send + Sync>>>> =
            Arc::new(RwLock::new(None));
        let cluster_id = AtomicU64::new(0);

        // 初始化 cluster_id
        {
            let mut client = PdClient::new(channel.clone());
            let response = client
                .get_members(Request::new(GetMembersRequest {
                    header: Some(RequestHeader {
                        cluster_id: 0,
                        sender_id: store_id,
                    }),
                }))
                .await?;
            let inner = response.into_inner();
            if let Some(header) = inner.header {
                cluster_id.store(header.cluster_id, Ordering::Relaxed);
            }
        }

        // 建立 RegionHeartbeat 流
        let mut hb_client = PdClient::new(channel.clone());
        let response = hb_client
            .region_heartbeat(Request::new(region_stream))
            .await?;
        let mut inbound = response.into_inner();

        let handler_clone = response_handler.clone();
        let tag_for_log = tag.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        log::info!("{}: heartbeat loop shutdown", tag_for_log);
                        break;
                    }
                    msg = inbound.message() => {
                        match msg {
                            Ok(Some(resp)) => {
                                if let Ok(handler) = handler_clone.read() {
                                    if let Some(cb) = handler.as_ref() {
                                        cb(resp);
                                    }
                                }
                            }
                            Ok(None) => break,
                            Err(err) => {
                                log::error!("heartbeat stream error: {}", err);
                                break;
                            }
                        }
                    }
                }
            }
        });

        let inner = Arc::new(ClientInner {
            tag,
            channel,
            cluster_id,
            store_id,
            region_tx,
            response_handler,
            shutdown_tx,
        });

        Ok(Self { inner })
    }

    fn new_pd_client(&self) -> PdClient<Channel> {
        PdClient::new(self.inner.channel.clone())
    }

    fn update_cluster_id(&self, header: &Option<schedulerpb::ResponseHeader>) {
        if let Some(h) = header {
            self.inner.cluster_id.store(h.cluster_id, Ordering::Relaxed);
        }
    }

    fn encode_store(&self, store: &Store) -> crate::proto::metapb::Store {
        // 转换为 schedulerpb 需要的格式
        // 由于使用了 extern_path，schedulerpb 中的 metapb 类型实际上就是 crate::proto::metapb
        crate::proto::metapb::Store {
            id: store.id,
            address: store.address.clone(),
            state: store.state,
        }
    }

    fn encode_peer(&self, peer: &Peer) -> crate::proto::metapb::Peer {
        crate::proto::metapb::Peer {
            id: peer.id,
            store_id: peer.store_id,
        }
    }

    fn encode_region(&self, region: &Region) -> crate::proto::metapb::Region {
        crate::proto::metapb::Region {
            id: region.id,
            start_key: region.start_key.clone(),
            end_key: region.end_key.clone(),
            region_epoch: region.region_epoch.clone(),
            peers: region.peers.iter().map(|p| self.encode_peer(p)).collect(),
        }
    }

    fn decode_store(&self, store: crate::proto::metapb::Store) -> Store {
        Store {
            id: store.id,
            address: store.address,
            state: store.state,
        }
    }

    fn decode_peer(&self, peer: crate::proto::metapb::Peer) -> Peer {
        Peer {
            id: peer.id,
            store_id: peer.store_id,
        }
    }

    fn decode_region(&self, region: crate::proto::metapb::Region) -> Region {
        Region {
            id: region.id,
            start_key: region.start_key,
            end_key: region.end_key,
            region_epoch: region.region_epoch,
            peers: region.peers.into_iter().map(|p| self.decode_peer(p)).collect(),
        }
    }
}

#[async_trait::async_trait]
impl SchedulerClient for Client {
    fn get_cluster_id(&self) -> u64 {
        self.inner.cluster_id.load(Ordering::Relaxed)
    }

    fn request_header(&self) -> RequestHeader {
        RequestHeader {
            cluster_id: self.get_cluster_id(),
            sender_id: self.inner.store_id,
        }
    }

    async fn alloc_id(&self) -> Result<u64> {
        let mut client = self.new_pd_client();
        let resp = client
            .alloc_id(Request::new(AllocIdRequest {
                header: Some(self.request_header()),
            }))
            .await?;
        let inner = resp.into_inner();
        self.update_cluster_id(&inner.header);
        Ok(inner.id)
    }

    async fn bootstrap(&self, store: &Store) -> Result<BootstrapResponse> {
        let mut client = self.new_pd_client();
        let resp = client
            .bootstrap(Request::new(BootstrapRequest {
                header: Some(self.request_header()),
                store: Some(self.encode_store(store)),
            }))
            .await?;
        let inner = resp.into_inner();
        self.update_cluster_id(&inner.header);
        Ok(inner)
    }

    async fn is_bootstrapped(&self) -> Result<bool> {
        let mut client = self.new_pd_client();
        let resp = client
            .is_bootstrapped(Request::new(IsBootstrappedRequest {
                header: Some(self.request_header()),
            }))
            .await?;
        let inner = resp.into_inner();
        self.update_cluster_id(&inner.header);
        Ok(inner.bootstrapped)
    }

    async fn put_store(&self, store: &Store) -> Result<()> {
        let mut client = self.new_pd_client();
        let resp = client
            .put_store(Request::new(PutStoreRequest {
                header: Some(self.request_header()),
                store: Some(self.encode_store(store)),
            }))
            .await?;
        self.update_cluster_id(&resp.into_inner().header);
        Ok(())
    }

    async fn get_store(&self, store_id: u64) -> Result<Store> {
        let mut client = self.new_pd_client();
        let resp = client
            .get_store(Request::new(GetStoreRequest {
                header: Some(self.request_header()),
                store_id,
            }))
            .await?;
        let inner = resp.into_inner();
        self.update_cluster_id(&inner.header);
        inner
            .store
            .map(|s| self.decode_store(s))
            .ok_or_else(|| anyhow!("store not found"))
    }

    async fn get_region(&self, key: &[u8]) -> Result<(Region, Option<Peer>)> {
        let mut client = self.new_pd_client();
        let resp = client
            .get_region(Request::new(GetRegionRequest {
                header: Some(self.request_header()),
                region_key: key.to_vec(),
            }))
            .await?;
        let inner = resp.into_inner();
        self.update_cluster_id(&inner.header);

        let region = inner
            .region
            .map(|r| self.decode_region(r))
            .ok_or_else(|| anyhow!("region not found"))?;
        let leader = inner.leader.map(|p| self.decode_peer(p));
        Ok((region, leader))
    }

    async fn get_region_by_id(&self, region_id: u64) -> Result<(Region, Option<Peer>)> {
        let mut client = self.new_pd_client();
        let resp = client
            .get_region_by_id(Request::new(GetRegionByIdRequest {
                header: Some(self.request_header()),
                region_id,
            }))
            .await?;
        let inner = resp.into_inner();
        self.update_cluster_id(&inner.header);
        let region = inner
            .region
            .map(|r| self.decode_region(r))
            .ok_or_else(|| anyhow!("region not found"))?;
        let leader = inner.leader.map(|p| self.decode_peer(p));
        Ok((region, leader))
    }

    async fn ask_split(&self, region: &Region) -> Result<AskSplitResponse> {
        let mut client = self.new_pd_client();
        let resp = client
            .ask_split(Request::new(AskSplitRequest {
                header: Some(self.request_header()),
                region: Some(self.encode_region(region)),
            }))
            .await?;
        let inner = resp.into_inner();
        self.update_cluster_id(&inner.header);
        Ok(inner)
    }

    async fn store_heartbeat(&self, stats: &StoreStats) -> Result<()> {
        let mut client = self.new_pd_client();
        let resp = client
            .store_heartbeat(Request::new(StoreHeartbeatRequest {
                header: Some(self.request_header()),
                stats: Some(stats.clone()),
            }))
            .await?;
        self.update_cluster_id(&resp.into_inner().header);
        Ok(())
    }

    fn region_heartbeat(&self, request: RegionHeartbeatRequest) -> Result<()> {
        self.inner
            .region_tx
            .send(request)
            .map_err(|e| anyhow!("send region heartbeat failed: {}", e))
    }

    fn set_region_heartbeat_response_handler(
        &self,
        handler: Box<dyn Fn(RegionHeartbeatResponse) + Send + Sync>,
    ) {
        if let Ok(mut slot) = self.inner.response_handler.write() {
            *slot = Some(handler);
        }
    }

    async fn close(&self) {
        let _ = self.inner.shutdown_tx.send(());
    }
}