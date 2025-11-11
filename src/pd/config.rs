use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct PdConfig {
    pub cluster_id: u64,
    pub grpc_addr: String,
    pub http_addr: String,
    pub data_dir: PathBuf,
    pub advertise_client_urls: Vec<String>,
    pub advertise_peer_urls: Vec<String>,
}

impl PdConfig {
    pub fn new(
        cluster_id: u64,
        grpc_addr: String,
        http_addr: String,
        data_dir: PathBuf,
        advertise_client_urls: Vec<String>,
        advertise_peer_urls: Vec<String>,
    ) -> Self {
        Self {
            cluster_id,
            grpc_addr,
            http_addr,
            data_dir,
            advertise_client_urls,
            advertise_peer_urls,
        }
    }
}