use std::time::Duration;

use reqwest::StatusCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    Retry,
    DoNotRetry,
}

pub fn classify_status(status: StatusCode) -> RetryDecision {
    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        RetryDecision::Retry
    } else {
        RetryDecision::DoNotRetry
    }
}

pub fn retry_delay(attempt: u32) -> Duration {
    let capped = attempt.min(6);
    Duration::from_millis(100 * 2_u64.pow(capped))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_throttling_and_server_errors() {
        assert_eq!(
            classify_status(StatusCode::TOO_MANY_REQUESTS),
            RetryDecision::Retry
        );
        assert_eq!(
            classify_status(StatusCode::INTERNAL_SERVER_ERROR),
            RetryDecision::Retry
        );
    }

    #[test]
    fn does_not_retry_success_or_bad_request() {
        assert_eq!(classify_status(StatusCode::OK), RetryDecision::DoNotRetry);
        assert_eq!(
            classify_status(StatusCode::BAD_REQUEST),
            RetryDecision::DoNotRetry
        );
    }

    #[test]
    fn delay_grows_then_caps() {
        assert_eq!(retry_delay(0), Duration::from_millis(100));
        assert_eq!(retry_delay(3), Duration::from_millis(800));
        assert_eq!(retry_delay(10), Duration::from_millis(6400));
    }
}
