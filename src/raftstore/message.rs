use crate::proto::metapb::RegionEpoch;
use anyhow::Result;

/// 消息类型
#[derive(Debug, Clone)]
pub enum MsgType {
    RaftMessage,
    RaftCmd,
    Tick,
    SplitRegion,
    RegionApproximateSize,
    Start,
    Stop,
}

/// 回调函数类型
/// 注意：由于 FnOnce 不能实现 Debug，我们使用一个包装结构
pub struct Callback(Box<dyn FnOnce(Result<Vec<u8>>) + Send + 'static>);

impl Callback {
    pub fn new<F>(f: F) -> Self
    where
        F: FnOnce(Result<Vec<u8>>) + Send + 'static,
    {
        Self(Box::new(f))
    }
    
    pub fn call(self, result: Result<Vec<u8>>) {
        (self.0)(result)
    }
}

impl std::fmt::Debug for Callback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Callback").finish()
    }
}

/// 消息
#[derive(Debug)]
pub struct Msg {
    pub msg_type: MsgType,
    pub region_id: u64,
    pub data: MsgData,
}

/// 消息数据
#[derive(Debug)]
pub enum MsgData {
    RaftMessage(Box<dyn std::any::Any + Send>),
    RaftCmd {
        request: Vec<u8>,
        #[allow(dead_code)]
        callback: Option<Callback>,
    },
    SplitRegion {
        region_epoch: RegionEpoch,
        split_key: Vec<u8>,
        #[allow(dead_code)]
        callback: Option<Callback>,
    },
    ApproximateSize(u64),
    Empty,
}

/// Split Region 消息
#[derive(Debug, Clone)]
pub struct MsgSplitRegion {
    pub region_epoch: RegionEpoch,
    pub split_key: Vec<u8>,
}

impl Msg {
    pub fn new(msg_type: MsgType, region_id: u64, data: MsgData) -> Self {
        Self {
            msg_type,
            region_id,
            data,
        }
    }
}