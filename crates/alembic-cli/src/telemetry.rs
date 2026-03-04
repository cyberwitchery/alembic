use tracing_subscriber::EnvFilter;

#[cfg(test)]
use std::sync::{Mutex, OnceLock};

#[cfg(test)]
fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn build_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"))
}

pub(crate) fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(build_filter())
        .with_ansi(false)
        .without_time()
        .with_target(false)
        .compact()
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_filter_defaults_to_warn() {
        let _guard = env_lock().lock().unwrap();
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
        let _guard = env_lock().lock().unwrap();
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

    #[test]
    fn init_tracing_is_safe_to_call_more_than_once() {
        init_tracing();
        init_tracing();
    }
}
