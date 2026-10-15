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
    fn build_filter_reads_rust_log() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let old = std::env::var("RUST_LOG").ok();
        // (RUST_LOG, filter, warning): an unset RUST_LOG is not reported, a
        // rejected one is. a bare `trce` would parse as a target at trace level;
        // the misspelt level needs an explicit target to be rejected.
        let cases = [
            (None, &["warn"][..], None),
            (
                Some("info,alembic_engine=debug"),
                &["alembic_engine=debug", "info"][..],
                None,
            ),
            (
                Some("info,alembic_engine=trce"),
                &["warn"][..],
                Some("warning: ignoring RUST_LOG `info,alembic_engine=trce`: "),
            ),
        ];
        for (value, expected_directives, expected_warning) in cases {
            match value {
                Some(value) => std::env::set_var("RUST_LOG", value),
                None => std::env::remove_var("RUST_LOG"),
            }
            let (filter, warning) = build_filter();
            // the directives, in whatever order the filter renders them
            let filter = filter.to_string();
            let mut directives: Vec<&str> = filter.split(',').collect();
            directives.sort_unstable();
            assert_eq!(directives, expected_directives, "{value:?}");
            match expected_warning {
                None => assert_eq!(warning, None, "{value:?}"),
                Some(prefix) => {
                    let warning = warning.expect("a rejected RUST_LOG is reported");
                    assert!(
                        warning.starts_with(prefix) && warning.ends_with("; using `warn`"),
                        "{warning}"
                    );
                }
            }
        }
        if let Some(value) = old {
            std::env::set_var("RUST_LOG", value);
        } else {
            std::env::remove_var("RUST_LOG");
        }
    }
}
