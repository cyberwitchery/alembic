//! integration with various chat services

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

    fn notification_url(&self, base_url: &str) -> String {
        match self {
            ChatopsBackend::Slack { secret } => {
                format!("{}/services/{}", base_url, secret)
            }
            ChatopsBackend::Discord { token } => {
                format!("{}/api/webhooks/{}", base_url, token)
            }
        }
    }

    pub(crate) fn default_base_url(&self) -> &str {
        match self {
            ChatopsBackend::Slack { .. } => "https://hooks.slack.com",
            ChatopsBackend::Discord { .. } => "https://discord.com",
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
    notify_with_base_url(backend, notification, backend.default_base_url()).await
}

async fn notify_with_base_url(
    backend: &ChatopsBackend,
    notification: &Notification,
    base_url: &str,
) -> Result<(), anyhow::Error> {
    let client = reqwest::Client::new();

    let res = client
        .post(backend.notification_url(base_url))
        .header("Content-Type", "application/json")
        .body(backend.notification_message(notification).to_string())
        .send()
        .await?;

    match res.error_for_status() {
        Ok(_) => {
            println!("chatops notification sent ({})", backend.name());
            Ok(())
        }
        Err(e) => Err(anyhow::anyhow!(
            "chatops notification response error ({}): {}",
            backend.name(),
            e
        )),
    }
}

#[cfg(test)]
mod tests {
    use crate::app::chatops::{notify_with_base_url, ChatopsBackend, Notification};
    use httpmock::Method::POST;
    use httpmock::MockServer;
    use serde_json::json;

    #[tokio::test]
    async fn test_chatops_slack_notification() {
        let backend = ChatopsBackend::Slack {
            secret: "very_secret".to_string(),
        };
        let notification = Notification::Plan("test".to_string());

        let server = MockServer::start_async().await;
        let notified = server.mock(|when, then| {
            when.method(POST).path("/services/very_secret");
            then.status(200).json_body(json!({}));
        });

        notify_with_base_url(&backend, &notification, &server.base_url())
            .await
            .unwrap();

        notified.assert_calls(1);
    }

    #[tokio::test]
    async fn test_chatops_discord_notification() {
        let backend = ChatopsBackend::Discord {
            token: "very_token".to_string(),
        };
        let notification = Notification::Plan("test".to_string());

        let server = MockServer::start_async().await;
        let notified = server.mock(|when, then| {
            when.method(POST).path("/api/webhooks/very_token");
            then.status(200).json_body(json!({}));
        });

        notify_with_base_url(&backend, &notification, &server.base_url())
            .await
            .unwrap();

        notified.assert_calls(1);
    }

    #[tokio::test]
    async fn notify_returns_err_on_4xx_response() {
        let server = MockServer::start_async().await;
        let backend = ChatopsBackend::Slack {
            secret: "bad_secret".into(),
        };
        let notification = Notification::Plan("test".into());

        server
            .mock_async(|when, then| {
                when.method(POST).path("/services/bad_secret");
                then.status(404);
            })
            .await;

        let err = notify_with_base_url(&backend, &notification, &server.base_url())
            .await
            .unwrap_err();

        assert!(err.to_string().contains("Slack"));
    }

    #[test]
    fn slack_notification_url_includes_secret() {
        let backend = ChatopsBackend::Slack {
            secret: "abc123".into(),
        };
        assert_eq!(
            backend.notification_url(backend.default_base_url()),
            "https://hooks.slack.com/services/abc123"
        );
    }

    #[test]
    fn discord_notification_url_includes_token() {
        let backend = ChatopsBackend::Discord {
            token: "xyz789".into(),
        };
        assert_eq!(
            backend.notification_url(backend.default_base_url()),
            "https://discord.com/api/webhooks/xyz789"
        );
    }

    #[test]
    fn slack_message_format_is_blocks_with_mrkdwn() {
        let backend = ChatopsBackend::Slack { secret: "s".into() };
        let msg = backend.notification_message(&Notification::Plan("hello".into()));
        assert_eq!(
            msg,
            json!({
                "blocks": [{
                    "type": "section",
                    "text": { "type": "mrkdwn", "text": "hello" }
                }]
            })
        );
    }

    #[test]
    fn discord_message_format_is_content_field() {
        let backend = ChatopsBackend::Discord { token: "t".into() };
        let msg = backend.notification_message(&Notification::Plan("hello".into()));
        assert_eq!(msg, json!({ "content": "hello" }));
    }
}
