use std::time::Duration;

/// Small exponential backoff helper for reconnect throttling.
pub struct ExponentialBackoff {
    min_delay: Duration,
    max_delay: Duration,
    current_delay: Duration,
}

impl ExponentialBackoff {
    pub fn new(min_delay: Duration, max_delay: Duration) -> Self {
        Self {
            min_delay,
            max_delay,
            current_delay: min_delay,
        }
    }

    pub fn reset(&mut self) {
        self.current_delay = self.min_delay
    }

    pub fn next_delay(&mut self) -> Duration {
        let delay = self.current_delay;
        self.current_delay = self.current_delay.saturating_mul(2).min(self.max_delay);
        delay
    }
}
