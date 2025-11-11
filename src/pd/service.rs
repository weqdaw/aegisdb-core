use crate::pd::state::PdState;
use crate::proto::schedulerpb::{
    self, pd_server::Pd, AskSplitRequest, AskSplitResponse, AllocIdRequest, AllocIdResponse,
    BootstrapRequest, BootstrapResponse, GetMembersRequest, GetMembersResponse,
    GetRegionByIdRequest, GetRegionRequest, GetRegionResponse, GetStoreRequest,
    GetStoreResponse, IsBootstrappedRequest, IsBootstrappedResponse, PutStoreRequest,
    PutStoreResponse, RegionHeartbeatRequest, RegionHeartbeatResponse, StoreHeartbeatRequest,
    StoreHeartbeatResponse,
};
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};

pub struct PdService {
    state: Arc<PdState>,
}

impl PdService {
    pub fn new(state: Arc<PdState>) -> Self {
        Self { state }
    }

    fn heartbeat_sender(
        &self,
    ) -> (
        UnboundedSender<RegionHeartbeatRequest>,
        UnboundedReceiverStream<RegionHeartbeatRequest>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        (tx, UnboundedReceiverStream::new(rx))
    }
}

#[tonic::async_trait]
impl Pd for PdService {
    type RegionHeartbeatStream =
        UnboundedReceiverStream<Result<RegionHeartbeatResponse, Status>>;

    async fn bootstrap(
        &self,
        request: Request<BootstrapRequest>,
    ) -> Result<Response<BootstrapResponse>, Status> {
        let req = request.into_inner();
        let store = req
            .store
            .ok_or_else(|| Status::invalid_argument("store missing"))?;
        let header = self
            .state
            .bootstrap(crate::pd::state::from_proto_store(store))
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(BootstrapResponse { header: Some(header) }))
    }

    async fn is_bootstrapped(
        &self,
        _request: Request<IsBootstrappedRequest>,
    ) -> Result<Response<IsBootstrappedResponse>, Status> {
        Ok(Response::new(IsBootstrappedResponse {
            header: Some(self.state.response_ok()),
            bootstrapped: self.state.is_bootstrapped(),
        }))
    }

    async fn alloc_id(
        &self,
        _request: Request<AllocIdRequest>,
    ) -> Result<Response<AllocIdResponse>, Status> {
        let id = self.state.id_allocator().alloc();
        Ok(Response::new(AllocIdResponse {
            header: Some(self.state.response_ok()),
            id,
        }))
    }

    async fn put_store(
        &self,
        request: Request<PutStoreRequest>,
    ) -> Result<Response<PutStoreResponse>, Status> {
        let req = request.into_inner();
        let store = req
            .store
            .ok_or_else(|| Status::invalid_argument("store missing"))?;
        let header = self
            .state
            .put_store(crate::pd::state::from_proto_store(store))
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(PutStoreResponse { header: Some(header) }))
    }

    async fn get_store(
        &self,
        request: Request<GetStoreRequest>,
    ) -> Result<Response<GetStoreResponse>, Status> {
        let req = request.into_inner();
        let resp = self
            .state
            .get_store(req.store_id)
            .map_err(|e| Status::not_found(e.to_string()))?;
        Ok(Response::new(resp))
    }

    async fn get_region(
        &self,
        request: Request<GetRegionRequest>,
    ) -> Result<Response<GetRegionResponse>, Status> {
        let req = request.into_inner();
        let resp = self
            .state
            .get_region(req.region_key)
            .map_err(|e| Status::not_found(e.to_string()))?;
        Ok(Response::new(resp))
    }

    async fn get_region_by_id(
        &self,
        request: Request<GetRegionByIdRequest>,
    ) -> Result<Response<GetRegionResponse>, Status> {
        let req = request.into_inner();
        let resp = self
            .state
            .get_region_by_id(req.region_id)
            .map_err(|e| Status::not_found(e.to_string()))?;
        Ok(Response::new(resp))
    }

    async fn ask_split(
        &self,
        request: Request<AskSplitRequest>,
    ) -> Result<Response<AskSplitResponse>, Status> {
        let req = request.into_inner();
        let resp = self
            .state
            .ask_split(req)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(resp))
    }

    async fn store_heartbeat(
        &self,
        request: Request<StoreHeartbeatRequest>,
    ) -> Result<Response<StoreHeartbeatResponse>, Status> {
        let req = request.into_inner();
        let resp = self
            .state
            .handle_store_heartbeat(req)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(resp))
    }

    async fn region_heartbeat(
        &self,
        request: Request<tonic::Streaming<RegionHeartbeatRequest>>,
    ) -> Result<Response<Self::RegionHeartbeatStream>, Status> {
        let mut inbound = request.into_inner();

        // Raw channel PD will write plain responses into
        let (pd_tx_raw, mut pd_rx_raw) = mpsc::unbounded_channel::<RegionHeartbeatResponse>();
        // Outgoing channel that wraps responses into Result for tonic stream
        let (pd_tx_out, pd_rx_out) = mpsc::unbounded_channel::<Result<RegionHeartbeatResponse, Status>>();
        let pd_rx: UnboundedReceiverStream<Result<RegionHeartbeatResponse, Status>> =
            UnboundedReceiverStream::new(pd_rx_out);

        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            // forward PD internal responses into gRPC stream as Ok(...)
            let forward = pd_tx_out.clone();
            tokio::spawn(async move {
                while let Some(msg) = pd_rx_raw.recv().await {
                    if forward.send(Ok(msg)).is_err() {
                        break;
                    }
                }
            });

            // Bind on first valid heartbeat, then handle messages
            let mut bound_store: Option<u64> = None;
            while let Ok(Some(msg)) = inbound.message().await {
                if bound_store.is_none() {
                    if let Some(header) = msg.header.as_ref() {
                        let store_id = header.sender_id;
                        state.bind_region_stream(store_id, pd_tx_raw.clone());
                        bound_store = Some(store_id);
                    } else {
                        // No header; skip until a valid first message arrives
                        continue;
                    }
                }
                if let Err(err) = state.handle_region_heartbeat(msg) {
                    log::error!("handle region heartbeat failed: {}", err);
                }
            }
            if let Some(store_id) = bound_store {
                state.unbind_region_stream(store_id);
            }
        });

        Ok(Response::new(pd_rx))
    }

    async fn get_members(
        &self,
        _request: Request<GetMembersRequest>,
    ) -> Result<Response<GetMembersResponse>, Status> {
        Ok(Response::new(self.state.get_members()))
    }
}

// map() helper for stream
use tokio_stream::StreamExt;