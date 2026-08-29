use std::time::Duration;

/// Minimum supported dashboard refresh interval.
pub const MIN_REFRESH: Duration = Duration::from_secs(1);

const DEFAULT_TITLE: &str = "Axum Sentinel Monitor";
const DEFAULT_DESCRIPTION: &str =
    "Live process, runtime, system, and HTTP metrics for this Axum service.";
const DEFAULT_FOOTER: &str = "Powered by axum-sentinel-monitor.";
const LEGACY_FONT_URL: &str =
    "https://fonts.googleapis.com/css2?family=Roboto:wght@400;900&display=swap";
const LEGACY_CHART_JS_URL: &str =
    "https://cdn.jsdelivr.net/npm/chart.js@2.9/dist/Chart.bundle.min.js";

/// Configuration for [`crate::Monitor`].
#[derive(Clone, Debug)]
pub struct Config {
    /// Dashboard page title.
    pub title: String,
    /// Text displayed below the heading.
    pub description: String,
    /// Text displayed at the bottom of the page.
    pub footer: String,
    /// Root-relative path or absolute HTTP(S) URL. Empty uses the built-in icon.
    pub favicon_url: String,
    /// Interval used for browser polling and the snapshot cache TTL.
    pub refresh: Duration,
    /// Return JSON even when the client does not request it.
    pub api_only: bool,
    /// Retained for source compatibility. The embedded dashboard ignores this field.
    pub custom_head: String,
    /// Retained for source compatibility. The embedded dashboard loads no external font.
    pub font_url: String,
    /// Retained for source compatibility. Charts use the built-in Canvas implementation.
    pub chart_js_url: String,
    /// Route exposed by [`crate::Monitor::router`].
    pub route: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            title: DEFAULT_TITLE.into(),
            description: DEFAULT_DESCRIPTION.into(),
            footer: DEFAULT_FOOTER.into(),
            favicon_url: String::new(),
            refresh: Duration::from_secs(3),
            api_only: false,
            custom_head: String::new(),
            font_url: LEGACY_FONT_URL.into(),
            chart_js_url: LEGACY_CHART_JS_URL.into(),
            route: "/metrics".into(),
        }
    }
}

impl Config {
    /// Minimum supported dashboard refresh interval.
    pub const MIN_REFRESH: Duration = Duration::from_secs(1);

    pub(crate) fn normalized(mut self) -> Self {
        if self.title.is_empty() {
            self.title = DEFAULT_TITLE.into();
        }
        if self.description.is_empty() {
            self.description = DEFAULT_DESCRIPTION.into();
        }
        if self.footer.is_empty() {
            self.footer = DEFAULT_FOOTER.into();
        }
        if self.refresh.is_zero() {
            self.refresh = Duration::from_secs(3);
        }
        if self.refresh < MIN_REFRESH {
            self.refresh = MIN_REFRESH;
        }
        if !self.favicon_url.is_empty() && !valid_favicon_url(&self.favicon_url) {
            self.favicon_url.clear();
        }
        if self.route.is_empty() {
            self.route = "/metrics".into();
        } else if !self.route.starts_with('/') {
            self.route.insert(0, '/');
        }
        self
    }
}

fn valid_favicon_url(raw: &str) -> bool {
    let raw = raw.trim();
    if raw.starts_with('/') && !raw.starts_with("//") && !raw.starts_with("/\\") {
        return true;
    }
    let Some((scheme, rest)) = raw.split_once("://") else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return false;
    }
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    !host.is_empty() && !host.contains('@')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamps_refresh_and_rejects_unsafe_favicon() {
        let config = Config {
            refresh: Duration::from_millis(200),
            favicon_url: "javascript:alert(1)".into(),
            route: "status".into(),
            ..Config::default()
        }
        .normalized();
        assert_eq!(config.refresh, MIN_REFRESH);
        assert!(config.favicon_url.is_empty());
        assert_eq!(config.route, "/status");
    }
}
