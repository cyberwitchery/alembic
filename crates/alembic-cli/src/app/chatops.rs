//! integration with Slack and other chat services via webhook

use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum ChatopsBackend {
    Slack { secret: String },
}

impl ChatopsBackend {
    fn name(&self) -> &'static str {
        match self {
            Self::Slack { .. } => "Slack",
        }
    }
}

pub enum Notification {
    Plan(String),
}

pub async fn notify(
    backend: &ChatopsBackend,
    notification: &Notification,
) -> Result<(), anyhow::Error> {
    let client = reqwest::Client::new();
    let secret = match backend {
        ChatopsBackend::Slack { secret } => secret,
    };
    let text = match notification {
        Notification::Plan(plan) => plan.clone(),
    };

    let res = client
        .post(format!("https://hooks.slack.com/services/{}", secret))
        .body(
            json!({"blocks": [
                {
                    "type": "section",
                    "text": {
                    "type": "mrkdwn",
                    "text": text,
                }
            }]})
            .to_string(),
        )
        .send()
        .await?;

    match res.error_for_status() {
        Ok(_) => {
            println!("chatops notification sent ({})", backend.name());
            Ok(())
        }
        Err(e) => Err(anyhow::anyhow!(
            "chatops notification response error: {}",
            e
        )),
    }
}
