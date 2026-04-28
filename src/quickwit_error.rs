#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuickwitErrorKind {
    RateLimited,
    Server,
    PayloadTooLarge,
    Schema,
    IndexMissing,
    BadRequest,
    Network,
    Unknown,
}

pub fn classify_quickwit_failure(status: reqwest::StatusCode, body: &str) -> QuickwitErrorKind {
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return QuickwitErrorKind::RateLimited;
    }
    if status == reqwest::StatusCode::PAYLOAD_TOO_LARGE {
        return QuickwitErrorKind::PayloadTooLarge;
    }
    if status == reqwest::StatusCode::NOT_FOUND {
        return QuickwitErrorKind::IndexMissing;
    }
    if status.is_server_error() {
        return QuickwitErrorKind::Server;
    }
    let body = body.to_ascii_lowercase();
    if body.contains("schema") || body.contains("field") {
        return QuickwitErrorKind::Schema;
    }
    if status.is_client_error() {
        return QuickwitErrorKind::BadRequest;
    }
    QuickwitErrorKind::Unknown
}

#[cfg(test)]
mod tests {
    use reqwest::StatusCode;

    use super::*;

    #[test]
    fn classifies_common_quickwit_failures() {
        assert_eq!(
            classify_quickwit_failure(StatusCode::TOO_MANY_REQUESTS, ""),
            QuickwitErrorKind::RateLimited
        );
        assert_eq!(
            classify_quickwit_failure(StatusCode::PAYLOAD_TOO_LARGE, ""),
            QuickwitErrorKind::PayloadTooLarge
        );
        assert_eq!(
            classify_quickwit_failure(StatusCode::NOT_FOUND, ""),
            QuickwitErrorKind::IndexMissing
        );
        assert_eq!(
            classify_quickwit_failure(StatusCode::INTERNAL_SERVER_ERROR, ""),
            QuickwitErrorKind::Server
        );
        assert_eq!(
            classify_quickwit_failure(StatusCode::BAD_REQUEST, "schema mismatch"),
            QuickwitErrorKind::Schema
        );
        assert_eq!(
            classify_quickwit_failure(StatusCode::BAD_REQUEST, "bad request"),
            QuickwitErrorKind::BadRequest
        );
    }
}
