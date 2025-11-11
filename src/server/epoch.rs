// aegisdb/src/server/epoch.rs
use crate::proto::metapb::{Region, RegionEpoch};
use crate::proto::errorpb::{self, Error};

/// 检查 RegionEpoch 是否匹配
/// 
/// 对于 RawKV API，我们只需要检查 version（区域版本）
/// conf_ver（配置版本）主要用于 Raft 相关的操作
pub fn check_region_epoch(
    request_epoch: &Option<RegionEpoch>,
    current_region: &Region,
) -> Result<(), Error> {
    // 如果请求中没有 epoch，跳过检查（用于 standalone 模式）
    let Some(req_epoch) = request_epoch else {
        return Ok(());
    };

    // 当前 region 若没有携带 epoch，则不判错（兼容）
    let Some(current_epoch) = current_region.region_epoch.as_ref() else {
        return Ok(());
    };

    // 检查版本是否匹配
    // 对于 RawKV API，主要检查 version
    if req_epoch.version != current_epoch.version {
        return Err(Error {
            message: String::new(),
            not_leader: None,
            region_not_found: None,
            key_not_in_region: None,
            stale_epoch: Some(errorpb::StaleEpoch {
                new_regions: vec![current_region.clone()],
            }),
        });
    }

    // 可选：也检查 conf_ver（配置版本）
    // 在 standalone 模式下通常不需要
    if req_epoch.conf_ver != current_epoch.conf_ver {
        return Err(Error {
            message: String::new(),
            not_leader: None,
            region_not_found: None,
            key_not_in_region: None,
            stale_epoch: Some(errorpb::StaleEpoch {
                new_regions: vec![current_region.clone()],
            }),
        });
    }

    Ok(())
}

/// 检查 epoch 是否过时
pub fn is_epoch_stale(epoch: &RegionEpoch, check_epoch: &RegionEpoch) -> bool {
    epoch.version < check_epoch.version || epoch.conf_ver < check_epoch.conf_ver
}