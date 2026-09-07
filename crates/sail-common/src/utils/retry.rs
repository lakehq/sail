use std::future::Future;
use std::time::Duration;

use fastrace::Span;
use log::warn;

use crate::config;
use crate::telemetry::{SpanAttribute, TracingFutureExt, record_error};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryStep {
    /// The retry number. Retries are numbered from one; the initial attempt is zero.
    pub retry: usize,
    pub delay: Duration,
}

#[derive(Debug, Clone)]
pub struct RetrySchedule {
    next: usize,
    remaining: usize,
    kind: RetryScheduleKind,
}

#[derive(Debug, Clone)]
enum RetryScheduleKind {
    Fixed {
        delay: Duration,
    },
    ExponentialBackoff {
        delay: Duration,
        max_delay: Duration,
        factor: u32,
    },
}

impl Iterator for RetrySchedule {
    type Item = RetryStep;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let retry = self.next;
        self.next += 1;
        self.remaining -= 1;
        let delay = match &mut self.kind {
            RetryScheduleKind::Fixed { delay } => *delay,
            RetryScheduleKind::ExponentialBackoff {
                delay,
                max_delay,
                factor,
            } => {
                let current = *delay;
                *delay = std::cmp::min(delay.saturating_mul(*factor), *max_delay);
                current
            }
        };
        Some(RetryStep { retry, delay })
    }
}

impl RetryStrategy {
    pub async fn run<F, Fut, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send + 'static,
        E: std::fmt::Display + Send + 'static,
    {
        let mut retries = self.retries();
        let mut attempt = 0;
        loop {
            let span = Span::enter_with_local_parent("RetryStrategy::run")
                .with_property(|| (SpanAttribute::RETRY_ATTEMPT, attempt.to_string()));
            let result = f().in_span_with_recorder(span, record_error).await;
            match result {
                x @ Ok(_) => return x,
                Err(e) => {
                    warn!("retryable operation failed: {e}");
                    if let Some(step) = retries.next() {
                        tokio::time::sleep(step.delay).await;
                        attempt = step.retry;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Returns a finite schedule containing only retries after the initial attempt.
    ///
    /// The first item has retry number one. If `max_count` is zero, the schedule is empty.
    pub fn retries(&self) -> RetrySchedule {
        match self {
            Self::ExponentialBackoff {
                max_count,
                initial_delay,
                max_delay,
                factor,
            } => RetrySchedule {
                next: 1,
                remaining: *max_count,
                kind: RetryScheduleKind::ExponentialBackoff {
                    delay: *initial_delay,
                    max_delay: *max_delay,
                    factor: *factor,
                },
            },
            Self::Fixed { max_count, delay } => RetrySchedule {
                next: 1,
                remaining: *max_count,
                kind: RetryScheduleKind::Fixed { delay: *delay },
            },
        }
    }
}

impl From<&config::RetryStrategy> for RetryStrategy {
    fn from(config: &config::RetryStrategy) -> Self {
        match config {
            config::RetryStrategy::Fixed(config::FixedRetryStrategy {
                max_count,
                delay_secs,
            }) => Self::Fixed {
                max_count: *max_count,
                delay: Duration::from_secs(*delay_secs),
            },
            config::RetryStrategy::ExponentialBackoff(
                config::ExponentialBackoffRetryStrategy {
                    max_count,
                    initial_delay_secs,
                    max_delay_secs,
                    factor,
                },
            ) => Self::ExponentialBackoff {
                max_count: *max_count,
                initial_delay: Duration::from_secs(*initial_delay_secs),
                max_delay: Duration::from_secs(*max_delay_secs),
                factor: *factor,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::{RetryStep, RetryStrategy};

    #[test]
    fn fixed_schedule_contains_one_based_retries() {
        let strategy = RetryStrategy::Fixed {
            max_count: 3,
            delay: Duration::from_secs(5),
        };

        assert_eq!(
            strategy.retries().collect::<Vec<_>>(),
            vec![
                RetryStep {
                    retry: 1,
                    delay: Duration::from_secs(5),
                },
                RetryStep {
                    retry: 2,
                    delay: Duration::from_secs(5),
                },
                RetryStep {
                    retry: 3,
                    delay: Duration::from_secs(5),
                },
            ]
        );
    }

    #[test]
    fn zero_max_count_has_no_retries() {
        let strategy = RetryStrategy::Fixed {
            max_count: 0,
            delay: Duration::from_secs(5),
        };

        assert_eq!(strategy.retries().next(), None);
    }

    #[test]
    fn exponential_backoff_schedule_is_capped() {
        let strategy = RetryStrategy::ExponentialBackoff {
            max_count: 4,
            initial_delay: Duration::from_secs(2),
            max_delay: Duration::from_secs(5),
            factor: 2,
        };

        let retries = strategy
            .retries()
            .map(|step| (step.retry, step.delay))
            .collect::<Vec<_>>();
        assert_eq!(
            retries,
            vec![
                (1, Duration::from_secs(2)),
                (2, Duration::from_secs(4)),
                (3, Duration::from_secs(5)),
                (4, Duration::from_secs(5)),
            ]
        );
    }

    #[tokio::test]
    async fn run_performs_initial_attempt_and_scheduled_retries() {
        let strategy = RetryStrategy::Fixed {
            max_count: 2,
            delay: Duration::ZERO,
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let result: Result<(), &str> = strategy
            .run({
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::Relaxed);
                    async { Err("failed") }
                }
            })
            .await;

        assert_eq!(result, Err("failed"));
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }
}
