use std::time::Duration;

/// Configuration for a [`Monitor`](crate::Monitor).
#[derive(Clone, Debug)]
pub struct Config {
    /// Dashboard document title.
    pub title: String,
    /// Metric collection and browser polling interval.
    pub refresh: Duration,
    /// Return JSON for every request instead of serving the dashboard.
    pub api_only: bool,
    /// URL for the dashboard font stylesheet. Empty disables the stylesheet.
    pub font_url: String,
    /// URL for Chart.js. Empty disables the script.
    pub chart_js_url: String,
    /// Trusted HTML inserted verbatim at the end of `<head>`.
    ///
    /// Never populate this field from untrusted input.
    pub custom_head: String,
}

impl Config {
    pub const MIN_REFRESH: Duration = Duration::from_millis(200);

    pub(crate) fn normalized(mut self) -> Self {
        if self.title.is_empty() {
            self.title = Self::default().title;
        }
        if self.refresh.is_zero() {
            self.refresh = Self::default().refresh;
        } else if self.refresh < Self::MIN_REFRESH {
            self.refresh = Self::MIN_REFRESH;
        }
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            title: "Axum Sentinel Monitor".to_owned(),
            refresh: Duration::from_secs(3),
            api_only: false,
            font_url: "https://fonts.googleapis.com/css2?family=Roboto:wght@400;900&display=swap"
                .to_owned(),
            chart_js_url: "https://cdn.jsdelivr.net/npm/chart.js@2.9/dist/Chart.bundle.min.js"
                .to_owned(),
            custom_head: String::new(),
        }
    }
}
