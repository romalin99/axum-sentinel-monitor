use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use axum::extract::MatchedPath;
use axum::http::{Request, Response, StatusCode};
use pin_project_lite::pin_project;
use tower::{Layer, Service};

use crate::stats::{InFlightGuard, SharedStats};

/// Tower layer that records HTTP metrics for non-monitor requests.
#[derive(Clone)]
pub struct MonitorLayer {
    pub(crate) stats: Arc<SharedStats>,
    pub(crate) skip_path: Arc<str>,
}

impl<S> Layer<S> for MonitorLayer {
    type Service = MonitorService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MonitorService {
            inner,
            stats: Arc::clone(&self.stats),
            skip_path: Arc::clone(&self.skip_path),
        }
    }
}

#[derive(Clone)]
pub struct MonitorService<S> {
    inner: S,
    stats: Arc<SharedStats>,
    skip_path: Arc<str>,
}

pin_project! {
    /// Future returned by [`MonitorService`].
    ///
    /// The in-flight guard is installed in [`Service::call`], so dropping this
    /// future without polling still releases the in-flight counter.
    pub struct MonitorFuture<F> {
        #[pin]
        inner: F,
        recording: Option<Recording>,
    }
}

struct Recording {
    guard: Option<InFlightGuard>,
    started: Instant,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for MonitorService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = MonitorFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        if request.uri().path() == self.skip_path.as_ref() {
            return MonitorFuture {
                inner: self.inner.call(request),
                recording: None,
            };
        }

        let stats = Arc::clone(&self.stats);
        // The route slot is resolved once here; the handle carries it through the
        // rest of the request so neither the completion nor the in-flight guard
        // has to normalize the path or take the route table lock again.
        let route = {
            let method = request.method().as_str();
            let path = request
                .extensions()
                .get::<MatchedPath>()
                .map_or_else(|| request.uri().path(), MatchedPath::as_str);
            stats.http().begin_request(method, path)
        };
        let started = Instant::now();
        let inner = self.inner.call(request);
        MonitorFuture {
            inner,
            recording: Some(Recording {
                guard: Some(InFlightGuard { stats, route }),
                started,
            }),
        }
    }
}

impl<F, B, E> Future for MonitorFuture<F>
where
    F: Future<Output = Result<Response<B>, E>>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let result = match this.inner.as_mut().poll(cx) {
            Poll::Ready(result) => result,
            Poll::Pending => return Poll::Pending,
        };
        if let Some(recording) = this.recording.as_mut()
            && let Some(guard) = recording.guard.take()
        {
            let status = match &result {
                Ok(response) => response.status(),
                Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
            };
            guard
                .stats
                .http()
                .finish(&guard.route, recording.started.elapsed(), status);
        }
        Poll::Ready(result)
    }
}
