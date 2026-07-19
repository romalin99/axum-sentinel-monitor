use std::{
    fmt,
    ops::{Deref, DerefMut},
};

use axum::{
    body::Bytes,
    extract::{FromRequest, Request},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Serialize, de::DeserializeOwned};

/// JSON extractor and response backed by [`sonic_rs`].
///
/// This is a drop-in equivalent for the common `axum::Json<T>` use case while
/// avoiding Axum's `json` feature and its `serde_json` dependency.
#[derive(Clone, Copy, Debug, Default)]
#[must_use]
pub struct SonicJson<T>(pub T);

impl<T, S> FromRequest<S> for SonicJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = SonicJsonRejection;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        if !is_json_content_type(request.headers()) {
            return Err(SonicJsonRejection::MissingContentType);
        }

        let bytes = Bytes::from_request(request, state)
            .await
            .map_err(|error| SonicJsonRejection::Body(error.to_string()))?;
        Self::from_bytes(&bytes)
    }
}

impl<T> SonicJson<T>
where
    T: DeserializeOwned,
{
    /// Deserializes a JSON document with `sonic-rs`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SonicJsonRejection> {
        sonic_rs::from_slice(bytes)
            .map(Self)
            .map_err(|error| SonicJsonRejection::InvalidJson(error.to_string()))
    }
}

impl<T> IntoResponse for SonicJson<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        match sonic_rs::to_vec(&self.0) {
            Ok(bytes) => (
                [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                bytes,
            )
                .into_response(),
            Err(error) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                )],
                error.to_string(),
            )
                .into_response(),
        }
    }
}

impl<T> Deref for SonicJson<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for SonicJson<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> From<T> for SonicJson<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

/// Rejection returned by [`SonicJson`] request extraction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SonicJsonRejection {
    /// The request has no JSON-compatible `Content-Type`.
    MissingContentType,
    /// Axum could not buffer the request body.
    Body(String),
    /// `sonic-rs` could not deserialize the request body.
    InvalidJson(String),
}

impl SonicJsonRejection {
    fn status(&self) -> StatusCode {
        match self {
            Self::MissingContentType => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::Body(_) => StatusCode::BAD_REQUEST,
            Self::InvalidJson(_) => StatusCode::BAD_REQUEST,
        }
    }
}

impl fmt::Display for SonicJsonRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingContentType => formatter.write_str(
                "expected request with `Content-Type: application/json` or `application/*+json`",
            ),
            Self::Body(error) => write!(formatter, "failed to read JSON body: {error}"),
            Self::InvalidJson(error) => write!(formatter, "invalid JSON: {error}"),
        }
    }
}

impl std::error::Error for SonicJsonRejection {}

impl IntoResponse for SonicJsonRejection {
    fn into_response(self) -> Response {
        (
            self.status(),
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            )],
            self.to_string(),
        )
            .into_response()
    }
}

fn is_json_content_type(headers: &HeaderMap) -> bool {
    let Some(content_type) = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
    else {
        return false;
    };
    let Some((kind, subtype)) = content_type.split_once('/') else {
        return false;
    };

    kind.eq_ignore_ascii_case("application")
        && (subtype.eq_ignore_ascii_case("json")
            || subtype
                .to_ascii_lowercase()
                .strip_suffix("+json")
                .is_some_and(|prefix| !prefix.is_empty()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_json_media_types() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "application/problem+json; charset=utf-8".parse().unwrap(),
        );
        assert!(is_json_content_type(&headers));

        headers.insert(header::CONTENT_TYPE, "text/json".parse().unwrap());
        assert!(!is_json_content_type(&headers));
    }
}
