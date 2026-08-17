//! integration with Slack and other chat services via webhook

use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum ChatopsBackend {
    Slack { secret: String },
    Discord { token: String },
}

impl ChatopsBackend {
    fn name(&self) -> &'static str {
        match self {
            Self::Slack { .. } => "Slack",
            Self::Discord { .. } => "Discord",
        }
    }

    fn notification_url(&self) -> String {
        match self {
            ChatopsBackend::Slack { secret } => {
                format!("https://hooks.slack.com/services/{}", secret)
            }
            ChatopsBackend::Discord { token } => {
                format!("https://discord.com/api/webhooks/{}", token)
            }
        }
    }

    fn notification_message(&self, notification: &Notification) -> serde_json::Value {
        match self {
            ChatopsBackend::Slack { .. } => {
                json!({"blocks": [
                    {
                        "type": "section",
                        "text": {
                        "type": "mrkdwn",
                        "text": notification.text(),
                    }
                }]})
            }
            ChatopsBackend::Discord { .. } => {
                json!({"content":
                    notification.text()
                })
            }
        }
    }
}

pub enum Notification {
    Plan(String),
}

impl Notification {
    fn text(&self) -> String {
        match self {
            Notification::Plan(plan) => plan.clone(),
        }
    }
}

pub async fn notify(
    backend: &ChatopsBackend,
    notification: &Notification,
) -> Result<(), anyhow::Error> {
    let client = reqwest::Client::new();

    println!(
        "Sending notification to url: {}",
        backend.notification_url()
    );

    let res = client
        .post(backend.notification_url())
        .header("Content-Type", "application/json")
        .body(backend.notification_message(notification).to_string())
        .send()
        .await?;

    match res.error_for_status() {
        Ok(ok) => {
            println!("chatops notification sent ({}): {:?}", backend.name(), ok);
            Ok(())
        }
        Err(e) => Err(anyhow::anyhow!(
            "chatops notification response error ({}): {}",
            backend.name(),
            e
        )),
    }
}
