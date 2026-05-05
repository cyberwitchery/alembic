//! configuration for the cli tool

use std::fmt::Display;
use serde::{Serialize, Deserialize};
use figment::{Figment, providers::{Format, Toml, Env}};
use figment::providers::Serialized;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct AppConfig {
    pub plugin_search_paths: Vec<String>,
}

impl AppConfig {
    fn default() -> AppConfig {
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