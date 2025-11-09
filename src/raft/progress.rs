/// Progress 表示 follower 在 leader 视角中的复制进度
#[derive(Debug, Clone)]
pub struct Progress {
    /// Match: 已确认复制的最高索引
    pub match_index: u64,
    /// Next: 下一个要发送的索引
    pub next_index: u64,
}

impl Progress {
    pub fn new(next_index: u64) -> Self {
        Self {
            match_index: 0,
            next_index,
        }
    }

    /// 更新 match_index
    pub fn update_match(&mut self, match_index: u64) {
        self.match_index = match_index;
        self.next_index = match_index + 1;
    }

    /// 更新 next_index（当复制失败时）
    pub fn decrease_next(&mut self, rejected_index: u64) {
        if self.next_index > 1 && self.next_index > rejected_index + 1 {
            self.next_index = rejected_index + 1;
        } else {
            self.next_index -= 1;
        }
    }
}