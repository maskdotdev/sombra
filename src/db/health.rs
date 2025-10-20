use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Check {
    CacheHitRate {
        current: f64,
        threshold: f64,
        healthy: bool,
    },
    WalSize {
        bytes: u64,
        threshold: u64,
        healthy: bool,
    },
    CorruptionErrors {
        count: u64,
        healthy: bool,
    },
    LastCheckpoint {
        seconds_ago: u64,
        threshold: u64,
        healthy: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck {
    pub status: HealthStatus,
    pub checks: Vec<Check>,
}

impl HealthCheck {
    pub fn new() -> Self {
        Self {
            status: HealthStatus::Healthy,
            checks: Vec::new(),
        }
    }

    pub fn add_check(&mut self, check: Check) {
        let is_healthy = match &check {
            Check::CacheHitRate { healthy, .. }
            | Check::WalSize { healthy, .. }
            | Check::CorruptionErrors { healthy, .. }
            | Check::LastCheckpoint { healthy, .. } => *healthy,
        };

        if !is_healthy {
            self.status = match self.status {
                HealthStatus::Healthy => HealthStatus::Degraded,
                HealthStatus::Degraded => HealthStatus::Degraded,
                HealthStatus::Unhealthy => HealthStatus::Unhealthy,
            };

            if matches!(
                check,
                Check::CorruptionErrors { count, .. } if count > 0
            ) {
                self.status = HealthStatus::Unhealthy;
            }
        }

        self.checks.push(check);
    }

    pub fn is_healthy(&self) -> bool {
        self.status == HealthStatus::Healthy
    }
}

impl Default for HealthCheck {
    fn default() -> Self {
        Self::new()
    }
}
