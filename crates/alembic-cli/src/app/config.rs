//! configuration for the cli tool

use crate::app::chatops::ChatopsBackend;
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
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub plugins_dir: PathBuf,
    pub chatops_backend: Option<ChatopsBackend>,
}

impl AppConfig {
    fn figment() -> Figment {
        // `app::state` shares the prefix and reads its variables directly, so taking
        // only this struct's keys keeps `deny_unknown_fields` off `ALEMBIC_STATE_*`.
        Figment::from(Serialized::defaults(Self::default()))
            .merge(Yaml::file("alembic.yaml"))
            .merge(Yaml::file("alembic.yml"))
            .merge(Env::prefixed("ALEMBIC_").only(&["plugins_dir"]))
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
            chatops_backend: None,
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
