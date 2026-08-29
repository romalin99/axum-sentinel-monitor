use crate::Config;

const DASHBOARD: &str = include_str!("dashboard.html");

const DEFAULT_FAVICON: &str = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCA2NCA2NCI+CjxyZWN0IHdpZHRoPSI2NCIgaGVpZ2h0PSI2NCIgcng9IjE0IiBmaWxsPSIjMGYxNzJhIi8+CjxwYXRoIGQ9Ik04IDM0aDEzbDYtMTcgMTEgMzIgNy0yMCA0IDVoNyIgZmlsbD0ibm9uZSIgc3Ryb2tlPSIjNjdlOGY5IiBzdHJva2Utd2lkdGg9IjYiIHN0cm9rZS1saW5lY2FwPSJyb3VuZCIgc3Ryb2tlLWxpbmVqb2luPSJyb3VuZCIvPgo8L3N2Zz4=";

pub(crate) fn render(config: &Config) -> String {
    let refresh_ms = config
        .refresh
        .as_millis()
        .clamp(1_000, i32::MAX as u128)
        .to_string();
    let favicon = if config.favicon_url.is_empty() {
        DEFAULT_FAVICON
    } else {
        config.favicon_url.as_str()
    };
    let descriptor = if cfg!(windows) { "Handles" } else { "FDs" };

    DASHBOARD
        .replace("__MONITOR_TITLE__", &escape_html(&config.title))
        .replace("__MONITOR_DESCRIPTION__", &escape_html(&config.description))
        .replace("__MONITOR_FOOTER__", &escape_html(&config.footer))
        .replace("__MONITOR_FAVICON_URL__", &escape_attr(favicon))
        .replace("__MONITOR_REFRESH_MS__", &refresh_ms)
        .replace("__MONITOR_DESCRIPTOR_LABEL__", descriptor)
        .replace("__MONITOR_PID__", &std::process::id().to_string())
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn escape_attr(value: &str) -> String {
    escape_html(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_title_and_ignores_custom_head() {
        let config = Config {
            title: "<unsafe>".into(),
            custom_head: "<meta name=\"x\">".into(),
            ..Config::default()
        };
        let html = render(&config);
        assert!(html.contains("&lt;unsafe&gt;"));
        assert!(!html.contains("<title><unsafe></title>"));
        assert!(!html.contains("<meta name=\"x\">"));
        assert!(!html.contains("Chart.js"));
        assert!(html.contains("Process"));
        assert!(html.contains("Runtime"));
        assert!(html.contains("System"));
        assert!(html.contains("Endpoints"));
        assert!(html.contains("data-filter=\"all\""));
        assert!(html.contains("Success"));
        assert!(html.contains("Redirect"));
        assert!(html.contains("endpoint-live"));
        assert!(html.contains("endpoint-stats"));
        assert!(html.contains(">P50<"));
        assert!(html.contains(">P95<"));
        assert!(html.contains(">P99<"));
        assert!(html.contains(">P999<"));
        assert!(html.contains(">30s<"));
        assert!(html.contains(">60s<"));
        assert!(!html.contains("data-samples=\"90\""));
        assert!(!html.contains("Location"));
        assert!(!html.contains("Day of week"));
        assert!(html.contains("data-theme=\"dark\""));
        assert!(html.contains("--bg: #0d1117"));
    }
}
