use tracing_subscriber::EnvFilter;

/// returns `warn` plus a message when `RUST_LOG` is rejected, so a bad value stays
/// distinguishable from an unset one. a nudge, never an error.
fn build_filter() -> (EnvFilter, Option<String>) {
    let Ok(value) = std::env::var("RUST_LOG") else {
        return (EnvFilter::new("warn"), None);
    };
    match EnvFilter::try_new(&value) {
        Ok(filter) => (filter, None),
        Err(err) => (
            EnvFilter::new("warn"),
            Some(format!(
                "warning: ignoring RUST_LOG `{value}`: {err}; using `warn`"
            )),
        ),
    }
}

pub(crate) fn init_tracing() {
    let (filter, warning) = build_filter();
    // use init() to fail fast if tracing is already initialized
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .without_time()
        .with_target(false)
        .compact()
        .init();
    if let Some(msg) = warning {
        eprintln!("{msg}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // serialize tests that mutate the shared RUST_LOG env var.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn build_filter_defaults_to_warn() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let old = std::env::var("RUST_LOG").ok();
        std::env::remove_var("RUST_LOG");
        let (filter, warning) = build_filter();
        assert_eq!(filter.to_string(), "warn");
        assert!(warning.is_none(), "an unset RUST_LOG is not reported");
        if let Some(value) = old {
            std::env::set_var("RUST_LOG", value);
        } else {
            std::env::remove_var("RUST_LOG");
        }
    }

    #[test]
    fn build_filter_uses_rust_log() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let old = std::env::var("RUST_LOG").ok();
        std::env::set_var("RUST_LOG", "info,alembic_engine=debug");
        let filter = build_filter().0.to_string();
        assert!(filter.contains("info"));
        assert!(filter.contains("alembic_engine=debug"));
        if let Some(value) = old {
            std::env::set_var("RUST_LOG", value);
        } else {
            std::env::remove_var("RUST_LOG");
        }
    }

    #[test]
    fn build_filter_reports_a_rejected_rust_log() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let old = std::env::var("RUST_LOG").ok();
        // a bare `trce` would parse as a target at trace level; the misspelt level
        // needs an explicit target to be rejected.
        std::env::set_var("RUST_LOG", "info,alembic_engine=trce");
        let (filter, warning) = build_filter();
        assert_eq!(filter.to_string(), "warn");
        assert!(warning.is_some(), "a rejected RUST_LOG is reported");
        if let Some(value) = old {
            std::env::set_var("RUST_LOG", value);
        } else {
            std::env::remove_var("RUST_LOG");
        }
    }
}
