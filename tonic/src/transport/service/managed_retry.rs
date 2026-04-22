//! Tower [`Layer`] and [`Service`] that implement managed retries.
//!
//! The retry decision is entirely delegated to the hook registered via
//! [`crate::datadog::rpcteam::admin_only_set_custom_retry_hook`].  When no
//! hook is registered, or the hook returns [`RetryDecision::Undecided`], the
//! request is **not** retried (tonic has no built-in status-code-based retry
//! logic).
//!
//! # Body buffering
//!
//! gRPC request bodies are fully buffered in memory before the first attempt
//! so that the bytes can be replayed on retries.  This is safe for unary and
//! server-streaming RPCs whose request bodies are small and already held in
//! memory by the encoder.
//!
//! # gRPC trailers-only errors
//!
//! gRPC encodes application-level failures as HTTP/2 trailers.  For unary
//! RPCs that fail immediately the server sends a single HEADERS frame with
//! `END_STREAM` set, carrying `grpc-status` and `grpc-message`.  At the
//! transport layer this arrives as `Ok(http::Response)`, not `Err(…)`.
//! This service inspects the initial response headers for such trailers-only
//! errors and routes them through the same retry path as transport errors.
//!
//! [`Layer`]: tower_layer::Layer
//! [`Service`]: tower_service::Service

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt as _;
use tower_layer::Layer;
use tower_service::Service;

use crate::body::Body;
use crate::datadog::rpcteam::{new_retry_throttler, try_custom_retry, RetryPolicy, RetryThrottler};
use crate::Status;

/// Convenience alias so we don't have to spell out the full pin-box type
/// everywhere inside this module.
type RetryFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

// ── ManagedRetryLayer ─────────────────────────────────────────────────────────

/// A [`Layer`] that wraps a service with managed retry logic.
///
/// Apply this layer to a [`Channel`](crate::transport::Channel) (or any
/// compatible service) to enable the retry hook registered by the RPC team.
///
/// [`Layer`]: tower_layer::Layer
#[derive(Debug, Clone, Copy)]
pub struct ManagedRetryLayer;

impl<S> Layer<S> for ManagedRetryLayer {
    type Service = ManagedRetryService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ManagedRetryService {
            inner,
            throttler: Arc::from(new_retry_throttler()),
        }
    }
}

// ── ManagedRetryService ───────────────────────────────────────────────────────

/// A service that retries failed RPCs according to the globally registered
/// retry hook.
///
/// Created by [`ManagedRetryLayer`]; prefer using the layer rather than
/// constructing this directly.
pub struct ManagedRetryService<S> {
    inner: S,
    throttler: Arc<dyn RetryThrottler>,
}

impl<S: Clone> Clone for ManagedRetryService<S> {
    fn clone(&self) -> Self {
        ManagedRetryService {
            inner: self.inner.clone(),
            throttler: self.throttler.clone(),
        }
    }
}

impl<S: fmt::Debug> fmt::Debug for ManagedRetryService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedRetryService")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<S> Service<http::Request<Body>> for ManagedRetryService<S>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>> + Clone + Send + 'static,
    S::Error: Into<crate::BoxError>,
    S::Future: Send + 'static,
{
    type Response = http::Response<Body>;
    type Error = crate::BoxError;
    type Future = RetryFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        let throttler = self.throttler.clone();
        // Clone the inner service (which was poll_ready'd by the caller) for
        // use inside the async block.  We call poll_ready again on each
        // per-attempt clone to satisfy the tower contract.
        let inner = self.inner.clone();

        Box::pin(async move {
            // Deconstruct the request so we can reconstruct it for each
            // attempt.  `http::request::Parts` is not Clone (Extensions has
            // no Clone impl), so we preserve only the wire-relevant fields.
            let (parts, body) = req.into_parts();
            let method = parts.method.clone();
            let uri = parts.uri.clone();
            let version = parts.version;
            let headers = parts.headers.clone();

            // Buffer body bytes once; replayed for every attempt.
            let body_bytes = collect_body_bytes(body)
                .await
                .map_err(|s| Box::new(s) as crate::BoxError)?;

            let mut attempt: u32 = 0;
            loop {
                let is_retry = attempt > 0;

                // Rebuild the HTTP request for this attempt.
                let retry_req = build_request(
                    method.clone(),
                    uri.clone(),
                    version,
                    headers.clone(),
                    body_bytes.clone(),
                );

                // Each attempt gets a fresh clone so poll_ready can be
                // called independently.
                let mut svc = inner.clone();
                std::future::poll_fn(|cx| svc.poll_ready(cx).map_err(Into::into))
                    .await
                    .map_err(|e: crate::BoxError| e)?;

                throttler.attempt_started(is_retry);
                let result = svc.call(retry_req).await.map_err(Into::into);
                throttler.attempt_completed();

                // Determine the gRPC status for this attempt.
                //
                // For "trailers-only" responses (the common failure case for
                // unary RPCs), the grpc-status appears in the *initial* HTTP
                // response headers.  Transport failures arrive as Rust `Err`.
                let status: Status = match result {
                    Ok(ref response) => {
                        match Status::from_header_map(response.headers()) {
                            // Non-OK trailers-only error → handle below.
                            Some(s) if s.code() != crate::Code::Ok => s,
                            // OK status or normal body response → pass through.
                            _ => return result,
                        }
                    }
                    Err(err) => Status::from_error_generic(err),
                };

                let policy = match try_custom_retry(&status) {
                    // No hook, Undecided, or explicit NoRetry → surface error.
                    Err(()) | Ok(None) => return Err(Box::new(status)),
                    Ok(Some(p)) => p,
                };

                if throttler.throttle() {
                    return Err(Box::new(status));
                }

                attempt += 1;
                // `max_attempts` includes the initial attempt.
                if attempt >= policy.max_attempts {
                    return Err(Box::new(status));
                }

                let backoff = compute_backoff(&policy, attempt);
                tokio::time::sleep(backoff).await;
            }
        })
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Collects all bytes from a [`Body`] in a single pass.
///
/// For gRPC unary requests the body is a single length-prefixed frame already
/// in memory, so this completes without suspending in the common case.
async fn collect_body_bytes(body: Body) -> Result<Bytes, Status> {
    body.collect()
        .await
        .map(|collected| collected.to_bytes())
}

/// Reconstructs an [`http::Request<Body>`] from its wire-relevant components.
///
/// Extensions are intentionally dropped: they do not affect the wire protocol
/// and `http::Extensions` has no `Clone` implementation.
fn build_request(
    method: http::Method,
    uri: http::Uri,
    version: http::Version,
    headers: http::HeaderMap,
    body: Bytes,
) -> http::Request<Body> {
    let mut req = http::Request::new(Body::new(http_body_util::Full::new(body)));
    *req.method_mut() = method;
    *req.uri_mut() = uri;
    *req.version_mut() = version;
    *req.headers_mut() = headers;
    req
}

/// Computes the backoff duration for retry attempt `n` (1-indexed).
///
/// Formula: `min(initial_backoff × multiplier^(n−1), max_backoff)`.
pub(crate) fn compute_backoff(policy: &RetryPolicy, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1) as f64;
    let factor = policy.backoff_multiplier.powf(exponent);
    policy.initial_backoff.mul_f64(factor).min(policy.max_backoff)
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datadog::rpcteam::{
        admin_only_set_custom_retry_hook, admin_only_set_custom_retry_throttler, RetryDecision,
    };
    use crate::datadog::rpcteam::managed_retry_hooks::reset_for_testing;
    use std::sync::{Arc, Mutex};

    static GLOBAL_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn lock() -> std::sync::MutexGuard<'static, ()> {
        GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn policy(max_attempts: u32, initial_ms: u64, max_ms: u64, multiplier: f64) -> RetryPolicy {
        RetryPolicy {
            max_attempts,
            initial_backoff: Duration::from_millis(initial_ms),
            max_backoff: Duration::from_millis(max_ms),
            backoff_multiplier: multiplier,
        }
    }

    // ── compute_backoff ───────────────────────────────────────────────────────

    #[test]
    fn backoff_attempt_1_equals_initial() {
        let p = policy(3, 100, 1000, 2.0);
        assert_eq!(compute_backoff(&p, 1), Duration::from_millis(100));
    }

    #[test]
    fn backoff_doubles_on_second_retry() {
        let p = policy(4, 100, 1000, 2.0);
        assert_eq!(compute_backoff(&p, 2), Duration::from_millis(200));
    }

    #[test]
    fn backoff_is_capped_at_max() {
        let p = policy(10, 100, 250, 2.0);
        assert_eq!(compute_backoff(&p, 3), Duration::from_millis(250));
    }

    #[test]
    fn backoff_with_multiplier_one_is_constant() {
        let p = policy(5, 50, 500, 1.0);
        assert_eq!(compute_backoff(&p, 1), Duration::from_millis(50));
        assert_eq!(compute_backoff(&p, 4), Duration::from_millis(50));
    }

    #[test]
    fn backoff_attempt_0_returns_initial() {
        let p = policy(3, 100, 1000, 2.0);
        assert_eq!(compute_backoff(&p, 0), Duration::from_millis(100));
    }

    // ── collect_body_bytes ────────────────────────────────────────────────────

    #[tokio::test]
    async fn collect_empty_body_returns_empty_bytes() {
        let body = Body::empty();
        let bytes = collect_body_bytes(body).await.unwrap();
        assert!(bytes.is_empty());
    }

    #[tokio::test]
    async fn collect_full_body_returns_all_bytes() {
        let data = Bytes::from_static(b"hello gRPC");
        let body = Body::new(http_body_util::Full::new(data.clone()));
        let bytes = collect_body_bytes(body).await.unwrap();
        assert_eq!(bytes, data);
    }

    // ── build_request ─────────────────────────────────────────────────────────

    #[test]
    fn build_request_sets_all_fields() {
        use http::{HeaderValue, Method, Version};
        let method = Method::POST;
        let uri: http::Uri = "http://example.com/pkg.Svc/Method".parse().unwrap();
        let version = Version::HTTP_2;
        let mut headers = http::HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/grpc"));
        let body = Bytes::from_static(b"\x00\x00\x00\x00\x05hello");

        let req = build_request(method.clone(), uri.clone(), version, headers.clone(), body);

        assert_eq!(req.method(), &method);
        assert_eq!(req.uri(), &uri);
        assert_eq!(req.version(), version);
        assert_eq!(
            req.headers().get("content-type").unwrap(),
            "application/grpc"
        );
    }

    // ── ManagedRetryService with mock inner service ───────────────────────────

    /// Mock inner service that fails the first `fail_count` calls with a
    /// transport-level error, then succeeds.
    #[derive(Clone)]
    struct MockService {
        call_count: Arc<Mutex<u32>>,
        fail_count: u32,
        fail_status: Status,
    }

    impl MockService {
        fn new(fail_count: u32, fail_status: Status) -> Self {
            MockService {
                call_count: Arc::new(Mutex::new(0)),
                fail_count,
                fail_status,
            }
        }
        fn calls(&self) -> u32 {
            *self.call_count.lock().unwrap()
        }
    }

    impl Service<http::Request<Body>> for MockService {
        type Response = http::Response<Body>;
        type Error = crate::BoxError;
        type Future = RetryFuture<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: http::Request<Body>) -> Self::Future {
            let mut count = self.call_count.lock().unwrap();
            *count += 1;
            let n = *count;
            let fail_count = self.fail_count;
            let status = self.fail_status.clone();
            Box::pin(async move {
                if n <= fail_count {
                    Err(Box::new(status) as crate::BoxError)
                } else {
                    Ok(http::Response::new(Body::empty()))
                }
            })
        }
    }

    fn make_request() -> http::Request<Body> {
        let body = Body::new(http_body_util::Full::new(Bytes::from_static(b"test")));
        http::Request::builder()
            .method(http::Method::POST)
            .uri("http://localhost/test")
            .body(body)
            .unwrap()
    }

    #[tokio::test]
    async fn no_hook_means_no_retry() {
        let _g = lock();
        reset_for_testing();

        let mock = MockService::new(1, Status::unavailable("down"));
        let mut svc = ManagedRetryLayer.layer(mock.clone());

        let result = svc.call(make_request()).await;
        assert!(result.is_err());
        assert_eq!(mock.calls(), 1);
    }

    #[tokio::test]
    async fn hook_returning_no_retry_stops_immediately() {
        let _g = lock();
        reset_for_testing();

        admin_only_set_custom_retry_hook(
            |_| RetryDecision::NoRetry,
            policy(5, 1, 10, 1.0),
        )
        .unwrap();

        let mock = MockService::new(5, Status::unavailable("down"));
        let mut svc = ManagedRetryLayer.layer(mock.clone());

        let result = svc.call(make_request()).await;
        assert!(result.is_err());
        assert_eq!(mock.calls(), 1);
    }

    #[tokio::test]
    async fn hook_returning_retry_retries_until_success() {
        let _g = lock();
        reset_for_testing();

        admin_only_set_custom_retry_hook(|_| RetryDecision::Retry, policy(4, 1, 10, 1.0))
            .unwrap();

        let mock = MockService::new(2, Status::unavailable("transient"));
        let mut svc = ManagedRetryLayer.layer(mock.clone());

        let result = svc.call(make_request()).await;
        assert!(result.is_ok());
        assert_eq!(mock.calls(), 3);
    }

    #[tokio::test]
    async fn exhausting_max_attempts_returns_error() {
        let _g = lock();
        reset_for_testing();

        admin_only_set_custom_retry_hook(|_| RetryDecision::Retry, policy(3, 1, 10, 1.0))
            .unwrap();

        let mock = MockService::new(100, Status::internal("crash"));
        let mut svc = ManagedRetryLayer.layer(mock.clone());

        let result = svc.call(make_request()).await;
        assert!(result.is_err());
        assert_eq!(mock.calls(), 3);
    }

    #[tokio::test]
    async fn throttler_suppresses_retry() {
        let _g = lock();
        reset_for_testing();

        admin_only_set_custom_retry_hook(|_| RetryDecision::Retry, policy(5, 1, 10, 1.0))
            .unwrap();

        struct AlwaysThrottle;
        impl RetryThrottler for AlwaysThrottle {
            fn throttle(&self) -> bool {
                true
            }
            fn attempt_started(&self, _: bool) {}
            fn attempt_completed(&self) {}
        }
        admin_only_set_custom_retry_throttler(|| Box::new(AlwaysThrottle)).unwrap();

        let mock = MockService::new(5, Status::unavailable("busy"));
        let mut svc = ManagedRetryLayer.layer(mock.clone());

        let result = svc.call(make_request()).await;
        assert!(result.is_err());
        assert_eq!(mock.calls(), 1);
    }

    #[tokio::test]
    async fn attempt_callbacks_are_called_correctly() {
        let _g = lock();
        reset_for_testing();

        admin_only_set_custom_retry_hook(|_| RetryDecision::Retry, policy(3, 1, 10, 1.0))
            .unwrap();

        #[derive(Default, Clone)]
        struct TrackingThrottler {
            starts: Arc<Mutex<Vec<bool>>>,
            completions: Arc<Mutex<u32>>,
        }
        impl RetryThrottler for TrackingThrottler {
            fn throttle(&self) -> bool {
                false
            }
            fn attempt_started(&self, is_retry: bool) {
                self.starts.lock().unwrap().push(is_retry);
            }
            fn attempt_completed(&self) {
                *self.completions.lock().unwrap() += 1;
            }
        }

        let tracker = TrackingThrottler::default();
        let tracker_for_check = tracker.clone();
        admin_only_set_custom_retry_throttler(move || Box::new(tracker.clone())).unwrap();

        let mock = MockService::new(2, Status::unavailable("x"));
        let mut svc = ManagedRetryLayer.layer(mock);

        svc.call(make_request()).await.unwrap();

        let starts = tracker_for_check.starts.lock().unwrap().clone();
        assert_eq!(starts, vec![false, true, true]);
        assert_eq!(*tracker_for_check.completions.lock().unwrap(), 3);
    }

    #[tokio::test]
    async fn hook_undecided_means_no_retry() {
        let _g = lock();
        reset_for_testing();

        admin_only_set_custom_retry_hook(|_| RetryDecision::Undecided, policy(5, 1, 10, 1.0))
            .unwrap();

        let mock = MockService::new(1, Status::unavailable("x"));
        let mut svc = ManagedRetryLayer.layer(mock.clone());

        let result = svc.call(make_request()).await;
        assert!(result.is_err());
        assert_eq!(mock.calls(), 1);
    }
}
