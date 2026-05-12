//! configuration for the cli tool

use figment::providers::{Serialized, Yaml};
use figment::{
    providers::{Env, Format},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::Display;
use std::path::PathBuf;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct AppConfig {
    pub plugins_dir: PathBuf,
}

impl AppConfig {
    fn figment() -> Figment {
        Figment::from(Serialized::defaults(Self::default()))
            .merge(Yaml::file("alembic.yaml"))
            .merge(Yaml::file("alembic.yml"))
            .merge(Env::prefixed("ALEMBIC_"))
    }

    pub(crate) fn load() -> Result<AppConfig, AppConfigError> {
        match AppConfig::figment().extract() {
            Ok(config) => Ok(config),
            Err(error) => Err(AppConfigError::FigmentError(Box::new(error))),
        }
    }
}

impl Default for AppConfig {
    fn default() -> AppConfig {
        AppConfig {
            plugins_dir: "./plugins".into(),
        }
    }
}

#[derive(Debug)]
pub enum AppConfigError {
    FigmentError(Box<figment::Error>),
}

impl Error for AppConfigError {}

impl Display for AppConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Configuration error: ")?;
        match self {
            AppConfigError::FigmentError(err) => write!(f, "{}", err),
        }
    }
}
