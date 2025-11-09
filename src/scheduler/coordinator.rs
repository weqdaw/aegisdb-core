use crate::scheduler::cluster::BasicCluster;
use crate::scheduler::schedulers::Scheduler;
use crate::scheduler::operator_controller::OperatorController;
use std::sync::Arc;
use tokio::time::Duration;
use parking_lot::RwLock;
use std::collections::HashMap;

/// 协调器，管理所有调度器
pub struct Coordinator {
    cluster: Arc<BasicCluster>,
    schedulers: Arc<RwLock<HashMap<String, Box<dyn Scheduler>>>>,
    op_controller: Arc<OperatorController>,
    running: Arc<RwLock<bool>>,
}

impl Coordinator {
    pub fn new(cluster: Arc<BasicCluster>, op_controller: Arc<OperatorController>) -> Self {
        Self {
            cluster,
            schedulers: Arc::new(RwLock::new(HashMap::new())),
            op_controller,
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// 注册调度器
    pub fn register_scheduler(&self, scheduler: Box<dyn Scheduler>) {
        let mut schedulers = self.schedulers.write();
        schedulers.insert(scheduler.scheduler_type().to_string(), scheduler);
    }

    /// 启动协调器
    pub async fn start(&self) {
        *self.running.write() = true;
        let cluster = Arc::clone(&self.cluster);
        let schedulers = Arc::clone(&self.schedulers);
        let op_controller = Arc::clone(&self.op_controller);
        let running = Arc::clone(&self.running);

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;

                if !*running.read() {
                    break;
                }

                // 运行所有调度器
                let schedulers_read = schedulers.read();
                for scheduler in schedulers_read.values() {
                    if !scheduler.is_schedule_allowed(&cluster) {
                        continue;
                    }

                    // 检查是否已有该 Region 的操作
                    if let Some(op) = scheduler.schedule(&cluster) {
                        let ops = vec![op];
                        if op_controller.add_operator(ops) {
                            log::info!("Created operator via scheduler");
                        }
                    }
                }
            }
        });
    }

    /// 停止协调器
    pub fn stop(&self) {
        *self.running.write() = false;
    }

    /// 分发操作（从心跳调用）
    pub fn dispatch(&self, region: &crate::scheduler::cluster::RegionInfo, source: &str) {
        self.op_controller.dispatch(region, source);
    }

    /// 获取 OperatorController
    pub fn op_controller(&self) -> &OperatorController {
        &self.op_controller
    }
}