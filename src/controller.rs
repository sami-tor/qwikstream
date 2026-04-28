use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchLimits {
    pub max_batch_bytes: usize,
    pub concurrency: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerLimits {
    pub workers: usize,
}

#[derive(Debug, Clone)]
pub struct AdaptiveController {
    limits: BatchLimits,
    min_batch_bytes: usize,
    max_batch_bytes: usize,
    min_concurrency: usize,
    max_concurrency: usize,
}

impl AdaptiveController {
    pub fn new(memory_limit_bytes: u64) -> Self {
        let max_batch_bytes =
            ((memory_limit_bytes / 8) as usize).clamp(64 * 1024, 16 * 1024 * 1024);
        Self {
            limits: BatchLimits {
                max_batch_bytes: (1024 * 1024).min(max_batch_bytes),
                concurrency: 1,
            },
            min_batch_bytes: 64 * 1024,
            max_batch_bytes,
            min_concurrency: 1,
            max_concurrency: 8,
        }
    }

    pub fn limits(&self) -> BatchLimits {
        self.limits
    }

    pub fn observe_success(&mut self, latency: Duration, queue_pressure: f32) {
        self.observe_ingest_success(latency, queue_pressure);
    }

    pub fn observe_ingest_success(
        &mut self,
        latency: Duration,
        queue_pressure: f32,
    ) -> WorkerLimits {
        if latency < Duration::from_millis(250) && queue_pressure > 0.50 {
            self.limits.max_batch_bytes = (self.limits.max_batch_bytes
                + self.limits.max_batch_bytes / 4)
                .min(self.max_batch_bytes);
            self.limits.concurrency = (self.limits.concurrency + 1).min(self.max_concurrency);
        }
        WorkerLimits {
            workers: self.limits.concurrency,
        }
    }

    pub fn observe_pressure(&mut self) {
        self.observe_ingest_pressure();
    }

    pub fn observe_ingest_pressure(&mut self) -> WorkerLimits {
        self.limits.max_batch_bytes = (self.limits.max_batch_bytes / 2).max(self.min_batch_bytes);
        self.limits.concurrency = self
            .limits
            .concurrency
            .saturating_sub(1)
            .max(self.min_concurrency);
        WorkerLimits {
            workers: self.limits.concurrency,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_increases_limits_within_bounds() {
        let mut controller = AdaptiveController::new(512 * 1024 * 1024);
        let before = controller.limits();
        controller.observe_success(Duration::from_millis(20), 0.8);
        let after = controller.limits();

        assert!(after.max_batch_bytes > before.max_batch_bytes);
        assert!(after.concurrency > before.concurrency);
    }

    #[test]
    fn adaptive_pressure_returns_lower_worker_limit() {
        let mut controller = AdaptiveController::new(512 * 1024 * 1024);
        controller.observe_ingest_success(Duration::from_millis(20), 0.8);
        let before = controller.limits().concurrency;
        let after = controller.observe_ingest_pressure().workers;

        assert!(after < before);
    }

    #[test]
    fn pressure_decreases_limits() {
        let mut controller = AdaptiveController::new(512 * 1024 * 1024);
        controller.observe_success(Duration::from_millis(20), 0.8);
        let before = controller.limits();
        controller.observe_pressure();
        let after = controller.limits();

        assert!(after.max_batch_bytes < before.max_batch_bytes);
        assert!(after.concurrency <= before.concurrency);
    }
}
