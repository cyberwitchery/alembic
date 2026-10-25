//! netbox adapter crate.

mod netbox;

pub use netbox::NetBoxAdapter;

use alembic_adapter_sdk::http_unavailable;
use std::error::Error as StdError;

/// whether `err` says netbox could not be reached or refused the credentials.
pub fn is_unavailable(err: &(dyn StdError + 'static)) -> bool {
    if let Some(err) = err.downcast_ref::<::netbox::Error>() {
        return match err {
            // the client's reqwest, which need not be the one this crate links
            ::netbox::Error::Http(err) => http_unavailable(
                err.is_connect() || err.is_timeout(),
                err.status().map(|status| status.as_u16()),
            ),
            ::netbox::Error::ApiError { status, .. } => http_unavailable(false, Some(*status)),
            ::netbox::Error::Auth(_) => true,
            _ => false,
        };
    }
    err.downcast_ref::<reqwest::Error>()
        .is_some_and(reqwest_unavailable)
}

fn reqwest_unavailable(err: &reqwest::Error) -> bool {
    http_unavailable(
        err.is_connect() || err.is_timeout(),
        err.status().map(|status| status.as_u16()),
    )
}
