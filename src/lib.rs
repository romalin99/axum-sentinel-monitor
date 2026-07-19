//! Embeddable process and system monitoring for Axum applications.

mod config;
mod dashboard;
mod handler;
mod json;
mod metrics;

use std::{
    sync::Arc,
    task::{Context, Poll},
};

use axum::{Router, http::Request};
use tower::{Layer, Service};

pub use config::Config;
pub use json::{SonicJson, SonicJsonRejection};
pub use metrics::{
    MetricSample, MetricsCollector, OsMetrics, ProcessMetrics, Snapshot, SystemCollector,
};

use crate::{handler::HandlerState, metrics::Sampler};

/// An isolated monitor instance with its own sampler and request counter.
#[derive(Clone)]
pub struct Monitor {
    sampler: Arc<Sampler>,
    config: Config,
}

impl Monitor {
    /// Starts a monitor using the system collector.
    pub fn new(config: Config) -> Self {
        Self::with_collector(config, SystemCollector::default())
    }

    /// Starts a monitor with a caller-provided metrics source.
    pub fn with_collector<C>(config: Config, collector: C) -> Self
    where
        C: MetricsCollector,
    {
        let config = config.normalized();
        let sampler = Sampler::start(config.refresh, Box::new(collector));
        Self { sampler, config }
    }

    /// Returns a router serving the dashboard and JSON API at `/`.
    pub fn router(&self) -> Router {
        handler::router(HandlerState {
            config: self.config.clone(),
            metrics: Arc::clone(&self.sampler.state),
        })
    }

    /// Returns a layer that counts all requests passing through it.
    pub fn layer(&self) -> MonitorLayer {
        MonitorLayer {
            metrics: Arc::clone(&self.sampler.state),
        }
    }

    /// Returns the latest collected snapshot.
    pub fn snapshot(&self) -> Snapshot {
        self.sampler.state.snapshot()
    }

    /// Stops this instance's shared background sampler.
    pub fn close(&self) {
        self.sampler.close();
    }

    /// Returns the normalized configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }
}

impl Default for Monitor {
    fn default() -> Self {
        Self::new(Config::default())
    }
}

/// Tower layer that records application request counts for one monitor.
#[derive(Clone)]
pub struct MonitorLayer {
    metrics: Arc<metrics::MetricsState>,
}

impl<S> Layer<S> for MonitorLayer {
    type Service = MonitorService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MonitorService {
            inner,
            metrics: Arc::clone(&self.metrics),
        }
    }
}

/// Service produced by [`MonitorLayer`].
#[derive(Clone)]
pub struct MonitorService<S> {
    inner: S,
    metrics: Arc<metrics::MetricsState>,
}

impl<S, B> Service<Request<B>> for MonitorService<S>
where
    S: Service<Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        self.metrics.increment_requests();
        self.inner.call(request)
    }
}
