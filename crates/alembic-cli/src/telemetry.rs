use tracing_subscriber::EnvFilter;

fn build_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"))
}

pub(crate) fn init_tracing() {
    // Use init() to fail fast if tracing is already initialized
    tracing_subscriber::fmt()
        .with_env_filter(build_filter())
        .with_ansi(false)
        .without_time()
        .with_target(false)
        .compact()
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Serialize tests that mutate the shared RUST_LOG env var.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn build_filter_defaults_to_warn() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let old = std::env::var("RUST_LOG").ok();
        std::env::remove_var("RUST_LOG");
        assert_eq!(build_filter().to_string(), "warn");
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
        let filter = build_filter().to_string();
        assert!(filter.contains("info"));
        assert!(filter.contains("alembic_engine=debug"));
        if let Some(value) = old {
            std::env::set_var("RUST_LOG", value);
        } else {
            std::env::remove_var("RUST_LOG");
        }
    }
}
