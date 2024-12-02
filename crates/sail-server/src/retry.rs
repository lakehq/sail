use std::future::Future;
use std::time::Duration;

use log::warn;
use sail_common::config;

#[derive(Debug, Clone)]
pub enum RetryStrategy {
    Fixed {
        max_count: usize,
        delay: Duration,
    },
    ExponentialBackoff {
        max_count: usize,
        initial_delay: Duration,
        max_delay: Duration,
        factor: u32,
    },
}

struct ExponentialBackoffDelay {
    delay: Duration,
    max_delay: Duration,
    factor: u32,
}

impl Iterator for ExponentialBackoffDelay {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        let delay = self.delay;
        self.delay = std::cmp::min(delay * self.factor, self.max_delay);
        Some(delay)
    }
}

impl RetryStrategy {
    pub async fn run<F, Fut, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: std::fmt::Display + Send,
    {
        let mut delay = self.delay();
        loop {
            match f().await {
                x @ Ok(_) => return x,
                Err(e) => {
                    warn!("retryable operation failed: {e}");
                    if let Some(delay) = delay.next() {
                        tokio::time::sleep(delay).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }

    fn delay(&self) -> Box<dyn Iterator<Item = Duration> + Send> {
        match self {
            Self::ExponentialBackoff {
                max_count,
                initial_delay,
                max_delay,
                factor,
            } => Box::new(
                ExponentialBackoffDelay {
                    delay: *initial_delay,
                    max_delay: *max_delay,
                    factor: *factor,
                }
                .take(*max_count),
            ),
            Self::Fixed { max_count, delay } => {
                Box::new(std::iter::repeat(*delay).take(*max_count))
            }
        }
    }
}

impl From<&config::RetryStrategy> for RetryStrategy {
    fn from(config: &config::RetryStrategy) -> Self {
        match config {
            config::RetryStrategy::Fixed {
                max_count,
                delay_secs,
            } => Self::Fixed {
                max_count: *max_count,
                delay: Duration::from_secs(*delay_secs),
            },
            config::RetryStrategy::ExponentialBackoff {
                max_count,
                initial_delay_secs,
                max_delay_secs,
                factor,
            } => Self::ExponentialBackoff {
                max_count: *max_count,
                initial_delay: Duration::from_secs(*initial_delay_secs),
                max_delay: Duration::from_secs(*max_delay_secs),
                factor: *factor,
            },
        }
    }
}
