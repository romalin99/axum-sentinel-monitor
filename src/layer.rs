use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use axum::http::{Request, Response, StatusCode};
use tower::{Layer, Service};

use crate::stats::{InFlightGuard, SharedStats};

/// Tower layer that records HTTP metrics for non-monitor requests.
#[derive(Clone)]
pub struct MonitorLayer {
    pub(crate) stats: Arc<SharedStats>,
    pub(crate) skip_path: String,
}

impl<S> Layer<S> for MonitorLayer {
    type Service = MonitorService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MonitorService {
            inner,
            stats: Arc::clone(&self.stats),
            skip_path: self.skip_path.clone(),
        }
    }
}

#[derive(Clone)]
pub struct MonitorService<S> {
    inner: S,
    stats: Arc<SharedStats>,
    skip_path: String,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for MonitorService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        if request.uri().path() == self.skip_path {
            let future = self.inner.call(request);
            return Box::pin(future);
        }

        let stats = Arc::clone(&self.stats);
        let sequence = stats.http().begin_request();
        let started = Instant::now();
        let future = self.inner.call(request);
        Box::pin(async move {
            let _guard = InFlightGuard(Arc::clone(&stats));
            let result = future.await;
            let elapsed = started.elapsed();
            match &result {
                Ok(response) => stats.http().finish(sequence, elapsed, response.status()),
                Err(_) => stats
                    .http()
                    .finish(sequence, elapsed, StatusCode::INTERNAL_SERVER_ERROR),
            }
            result
        })
    }
}
