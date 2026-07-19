use std::sync::Arc;

use axum::{
    Router,
    extract::State,
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::any,
};

use crate::{Config, SonicJson, dashboard, metrics::MetricsState};

#[derive(Clone)]
pub(crate) struct HandlerState {
    pub(crate) config: Config,
    pub(crate) metrics: Arc<MetricsState>,
}

pub(crate) fn router(state: HandlerState) -> Router {
    Router::new().route("/", any(endpoint)).with_state(state)
}

async fn endpoint(
    State(state): State<HandlerState>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    if method != Method::GET {
        return (
            StatusCode::METHOD_NOT_ALLOWED,
            [(header::ALLOW, HeaderValue::from_static("GET"))],
        )
            .into_response();
    }

    let json = state.config.api_only || prefers_json(&headers);
    let mut response = if json {
        SonicJson(state.metrics.snapshot()).into_response()
    } else {
        Html(dashboard::render(&state.config)).into_response()
    };
    if json {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        );
    }
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    response
}

fn prefers_json(headers: &HeaderMap) -> bool {
    let Some(accept) = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let ranges = parse_accept(accept);
    let json = candidate_quality(&ranges, "application", "json");
    let html = candidate_quality(&ranges, "text", "html");
    match (json, html) {
        (Some(json), Some(html)) => json.0 > 0.0 && json > html,
        (Some((quality, _)), None) => quality > 0.0,
        _ => false,
    }
}

fn parse_accept(value: &str) -> Vec<(&str, &str, f32)> {
    value
        .split(',')
        .filter_map(|item| {
            let mut parts = item.trim().split(';');
            let (kind, subtype) = parts.next()?.trim().split_once('/')?;
            let quality = parts
                .find_map(|parameter| {
                    let (name, value) = parameter.trim().split_once('=')?;
                    name.eq_ignore_ascii_case("q")
                        .then(|| value.trim().parse::<f32>().ok())
                        .flatten()
                })
                .unwrap_or(1.0)
                .clamp(0.0, 1.0);
            Some((kind.trim(), subtype.trim(), quality))
        })
        .collect()
}

fn candidate_quality(
    ranges: &[(&str, &str, f32)],
    candidate_kind: &str,
    candidate_subtype: &str,
) -> Option<(f32, u8)> {
    ranges
        .iter()
        .filter_map(|(kind, subtype, quality)| {
            if (*kind == "*" || kind.eq_ignore_ascii_case(candidate_kind))
                && (*subtype == "*" || subtype.eq_ignore_ascii_case(candidate_subtype))
            {
                let specificity = u8::from(*kind != "*") + u8::from(*subtype != "*");
                Some((*quality, specificity))
            } else {
                None
            }
        })
        .max_by(|left, right| {
            left.1
                .cmp(&right.1)
                .then_with(|| left.0.total_cmp(&right.0))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiates_quality_and_specificity() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ACCEPT,
            "text/html;q=0.4, application/json;q=0.8".parse().unwrap(),
        );
        assert!(prefers_json(&headers));
        headers.insert(
            header::ACCEPT,
            "application/json;q=0, */*;q=1".parse().unwrap(),
        );
        assert!(!prefers_json(&headers));
    }
}
