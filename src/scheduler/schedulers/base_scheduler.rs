use crate::scheduler::operator::Operator;
use crate::scheduler::cluster::BasicCluster;

/// 调度器接口
pub trait Scheduler: Send + Sync {
    /// 调度器名称
    fn name(&self) -> &str;
    
    /// 调度器类型
    fn scheduler_type(&self) -> &str;
    
    /// 是否允许调度
    fn is_schedule_allowed(&self, cluster: &BasicCluster) -> bool;
    
    /// 执行调度，返回 Operator
    fn schedule(&self, cluster: &BasicCluster) -> Option<Operator>;
    
    /// 最小调度间隔（秒）
    fn min_interval(&self) -> u64 {
        3
    }
    
    /// 获取下一个调度间隔（秒）
    fn next_interval(&self, current: u64) -> u64 {
        (current as f64 * 1.5) as u64
    }
}