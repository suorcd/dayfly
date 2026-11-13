//! Prometheus metrics for HTTP mode.
//!
//! The registry exists only when `DAYFLY_METRICS_BIND` names a private
//! listener. The public API listener never serves `/metrics`.
//!
//! Labels use the route template, the method, and the status class. They
//! never use a paste ID, an alias, a client address, or a query string,
//! so the label cardinality stays bounded.

use std::time::Duration;

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
};

/// The process-wide metrics registry.
pub struct Metrics {
    registry: Registry,
    http_requests: IntCounterVec,
    http_request_duration: HistogramVec,
    http_requests_in_flight: IntGauge,
    http_request_bytes: IntCounterVec,
    http_response_bytes: IntCounterVec,
    paste_operations: IntCounterVec,
    s3_requests: IntCounterVec,
    rate_limited: IntCounter,
    multipart_orphans: IntCounter,
}

impl Metrics {
    /// Create the registry and register every metric.
    ///
    /// A registration failure is a programming error: the names and
    /// labels are fixed in this module.
    pub fn new() -> Self {
        let registry = Registry::new();

        let build_info = IntGaugeVec::new(
            Opts::new("dayfly_build_info", "Build information."),
            &["version"],
        )
        .expect("build info metric must be valid");
        build_info
            .with_label_values(&[env!("CARGO_PKG_VERSION")])
            .set(1);

        let http_requests = IntCounterVec::new(
            Opts::new(
                "dayfly_http_requests_total",
                "HTTP requests by route and status.",
            ),
            &["method", "route", "status"],
        )
        .expect("request counter must be valid");

        let http_request_duration = HistogramVec::new(
            HistogramOpts::new(
                "dayfly_http_request_duration_seconds",
                "HTTP request latency in seconds. The measurement stops when the response starts.",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
            ]),
            &["method", "route"],
        )
        .expect("request histogram must be valid");

        let http_requests_in_flight = IntGauge::new(
            "dayfly_http_requests_in_flight",
            "HTTP requests that the process is handling now.",
        )
        .expect("in-flight gauge must be valid");

        let http_request_bytes = IntCounterVec::new(
            Opts::new(
                "dayfly_http_request_bytes_total",
                "Declared request body bytes by route.",
            ),
            &["method", "route"],
        )
        .expect("request byte counter must be valid");

        let http_response_bytes = IntCounterVec::new(
            Opts::new(
                "dayfly_http_response_bytes_total",
                "Declared response body bytes by route.",
            ),
            &["method", "route"],
        )
        .expect("response byte counter must be valid");

        let paste_operations = IntCounterVec::new(
            Opts::new(
                "dayfly_paste_operations_total",
                "Paste API operations by operation and outcome.",
            ),
            &["operation", "outcome"],
        )
        .expect("paste operation counter must be valid");

        let s3_requests = IntCounterVec::new(
            Opts::new(
                "dayfly_s3_requests_total",
                "S3 requests by operation and outcome.",
            ),
            &["operation", "outcome"],
        )
        .expect("S3 request counter must be valid");

        let rate_limited = IntCounter::new(
            "dayfly_rate_limited_total",
            "Requests refused by the rate limiter.",
        )
        .expect("rate limit counter must be valid");

        let multipart_orphans = IntCounter::new(
            "dayfly_multipart_orphans_aborted_total",
            "Incomplete multipart uploads aborted by the stale-upload sweep.",
        )
        .expect("multipart orphan counter must be valid");

        let collectors: Vec<Box<dyn prometheus::core::Collector>> = vec![
            Box::new(build_info),
            Box::new(http_requests.clone()),
            Box::new(http_request_duration.clone()),
            Box::new(http_requests_in_flight.clone()),
            Box::new(http_request_bytes.clone()),
            Box::new(http_response_bytes.clone()),
            Box::new(paste_operations.clone()),
            Box::new(s3_requests.clone()),
            Box::new(rate_limited.clone()),
            Box::new(multipart_orphans.clone()),
        ];
        for collector in collectors {
            registry
                .register(collector)
                .expect("metric names must be unique");
        }
        registry
            .register(Box::new(
                prometheus::process_collector::ProcessCollector::for_self(),
            ))
            .expect("process collector must register");

        Self {
            registry,
            http_requests,
            http_request_duration,
            http_requests_in_flight,
            http_request_bytes,
            http_response_bytes,
            paste_operations,
            s3_requests,
            rate_limited,
            multipart_orphans,
        }
    }

    /// Render the registry in the Prometheus text format.
    pub fn render(&self) -> Result<Vec<u8>, prometheus::Error> {
        let mut buffer = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut buffer)?;
        Ok(buffer)
    }

    /// Count one request that started. The guard decrements the gauge when
    /// it drops, including when the request is cancelled.
    pub fn start_request(&self) -> InFlightGuard {
        self.http_requests_in_flight.inc();
        InFlightGuard {
            gauge: self.http_requests_in_flight.clone(),
        }
    }

    /// Record one completed response.
    pub fn observe_request(
        &self,
        method: &str,
        route: &str,
        status: u16,
        elapsed: Duration,
        request_bytes: u64,
        response_bytes: u64,
    ) {
        let status = status.to_string();
        self.http_requests
            .with_label_values(&[method, route, status.as_str()])
            .inc();
        self.http_request_duration
            .with_label_values(&[method, route])
            .observe(elapsed.as_secs_f64());
        self.http_request_bytes
            .with_label_values(&[method, route])
            .inc_by(request_bytes);
        self.http_response_bytes
            .with_label_values(&[method, route])
            .inc_by(response_bytes);
    }

    /// Record one paste API operation.
    pub fn observe_paste_operation(&self, operation: &str, outcome: &str) {
        self.paste_operations
            .with_label_values(&[operation, outcome])
            .inc();
    }

    /// Record one S3 request.
    pub fn observe_s3(&self, operation: &str, outcome: &str) {
        self.s3_requests
            .with_label_values(&[operation, outcome])
            .inc();
    }

    /// Record one request refused by the rate limiter.
    pub fn count_rate_limited(&self) {
        self.rate_limited.inc();
    }

    /// Record one stale multipart upload that the sweep aborted.
    pub fn count_multipart_orphan(&self) {
        self.multipart_orphans.inc();
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Keeps the in-flight gauge accurate when a request ends or is cancelled.
pub struct InFlightGuard {
    gauge: IntGauge,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}
