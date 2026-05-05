//! configuration for the cli tool

use figment::providers::Serialized;
use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct AppConfig {
    pub plugin_search_paths: Vec<String>,
}

impl AppConfig {
    pub fn default() -> AppConfig {
        AppConfig {
            plugin_search_paths: vec![
                "../../target/debug/examples/".to_string(), // For tests
                "../alembic-ops/target/debug/".to_string(), // For local usage
            ],
        }
    }
    fn figment() -> Figment {
        Figment::from(Serialized::defaults(Self::default()))
            .merge(Toml::file("alembic.toml"))
            .merge(Env::prefixed("ALEMBIC_"))
    }

    pub(crate) fn load() -> Result<AppConfig, AppConfigError> {
        match AppConfig::figment().extract() {
            Ok(config) => Ok(config),
            Err(error) => Err(AppConfigError::FigmentError(error)),
        }
    }
}

pub enum AppConfigError {
    FigmentError(figment::Error),
}

impl Display for AppConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Configuration error: ")?;
        match self {
            AppConfigError::FigmentError(err) => write!(f, "{}", err),
        }
    }
}
