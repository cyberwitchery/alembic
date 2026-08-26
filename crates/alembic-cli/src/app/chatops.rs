//! integration with various chat services

use alembic_core::key_string;
use alembic_engine::{Op, Plan};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::PathBuf;
use tracing;

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

    fn slack_header(text: &str) -> Value {
        json!({
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": text,
                "emoji": true
            }
        })
    }

    fn slack_bullet_point(text: &str) -> Value {
        json!({
            "type": "rich_text_section",
            "elements": [{ "type": "text", "text": text }]
        })
    }

    fn slack_bullet_list_block(points: &[BulletPoint]) -> Value {
        let mut elements: Vec<Value> = Vec::new();

        for point in points {
            // top-level bullet
            elements.push(json!({
                "type": "rich_text_list",
                "style": "bullet",
                "indent": 0,
                "elements": [Self::slack_bullet_point(&point.text)]
            }));

            // sub-bullets, if any, as an indented sibling list
            if !point.sub_points.is_empty() {
                elements.push(json!({
                    "type": "rich_text_list",
                    "style": "bullet",
                    "indent": 1,
                    "elements": point.sub_points
                        .iter()
                        .map(|p| Self::slack_bullet_point(p))
                        .collect::<Vec<_>>()
                }));
            }
        }

        json!({
            "type": "rich_text",
            "elements": elements
        })
    }

    fn slack_action_buttons(session_data: String) -> Value {
        // note that the data stored under the `value` key must be a String
        json!({
            "type": "actions",
            "block_id": "approval_actions",
            "elements": [
            {
                "type": "button",
                "text": {
                "type": "plain_text",
                "text": "Approve"
            },
                "style": "primary",
                "action_id": "approve_button",
                "value": session_data,
            },
            {
                "type": "button",
                "text": {
                "type": "plain_text",
                "text": "Deny"
            },
                "style": "danger",
                "action_id": "deny_button",
                "value": session_data,
            }
            ]
        })
    }

    fn notification_message(&self, notification: &Notification) -> anyhow::Result<Value> {
        let command_data_json = serde_json::to_string(&notification.command)?;
        match self {
            ChatopsBackend::Slack { .. } => {
                let mut blocks: Vec<Value> = notification
                    .sections
                    .iter()
                    .flat_map(|s| {
                        vec![
                            Self::slack_header(&s.title),
                            Self::slack_bullet_list_block(&s.bullet_points),
                        ]
                    })
                    .collect();

                if blocks.is_empty() {
                    blocks.push(Self::slack_header(
                        "The plan did not contain any ops to approve.",
                    ))
                } else {
                    blocks.push(Self::slack_action_buttons(command_data_json));
                }

                Ok(json!({ "blocks": blocks }))
            }
            ChatopsBackend::Discord { .. } => Ok(json!({"content":
                notification.text()
            })),
        }
    }
}

pub struct Notification {
    pub sections: Vec<NotificationSection>,
    pub command: CommandData,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CommandData {
    Plan {
        file: String,
        backend: Option<String>,
        backend_config: Option<PathBuf>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BulletPoint {
    pub text: String,
    pub sub_points: Vec<String>,
}

pub struct NotificationSection {
    pub title: String,
    pub bullet_points: Vec<BulletPoint>,
}

// general purpose container for notification data, not tied to a particular chat service
impl Notification {
    pub fn from_plan(
        plan: &Plan,
        plan_path: &str,
        backend: Option<String>,
        backend_config: Option<PathBuf>,
    ) -> Self {
        let mut sections: Vec<NotificationSection> = vec![];

        let creates = plan
            .ops
            .iter()
            .filter(|op| matches!(op, Op::Create { .. }))
            .collect::<Vec<_>>();
        let updates = plan
            .ops
            .iter()
            .filter(|op| matches!(op, Op::Update { .. }))
            .collect::<Vec<_>>();
        let deletes = plan
            .ops
            .iter()
            .filter(|op| matches!(op, Op::Delete { .. }))
            .collect::<Vec<_>>();

        if !creates.is_empty() {
            sections.push(NotificationSection {
                title: "Create".to_string(),
                bullet_points: creates
                    .iter()
                    .map(|op| match op {
                        Op::Create {
                            type_name, desired, ..
                        } => BulletPoint {
                            text: format!("{} {}", type_name, key_string(&desired.key)),
                            sub_points: vec![],
                        },
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>(),
            });
        }

        if !updates.is_empty() {
            sections.push(NotificationSection {
                title: "Update".to_string(),
                bullet_points: updates
                    .iter()
                    .map(|op| match op {
                        Op::Update {
                            type_name,
                            desired,
                            changes,
                            ..
                        } => BulletPoint {
                            text: format!("{} {}", type_name, key_string(&desired.key)),
                            sub_points: changes
                                .iter()
                                .map(|change| {
                                    format!("{}: {} -> {}", change.field, change.from, change.to)
                                })
                                .collect(),
                        },
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>(),
            });
        }

        if !deletes.is_empty() {
            sections.push(NotificationSection {
                title: "Delete".to_string(),
                bullet_points: deletes
                    .iter()
                    .map(|op| match op {
                        Op::Delete { type_name, key, .. } => BulletPoint {
                            text: format!("{} {}", type_name, key_string(key)),
                            sub_points: vec![],
                        },
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>(),
            });
        }

        Notification {
            sections,
            command: CommandData::Plan {
                file: plan_path.to_string(),
                backend,
                backend_config,
            },
        }
    }

    fn text(&self) -> String {
        self.sections.iter().map(|s| s.title.clone()).collect()
    }
}

pub async fn notify(
    chatops_backend: &ChatopsBackend,
    notification: &Notification,
) -> Result<(), anyhow::Error> {
    notify_with_base_url(
        chatops_backend,
        notification,
        chatops_backend.default_base_url(),
    )
    .await
}

async fn notify_with_base_url(
    chatops_backend: &ChatopsBackend,
    notification: &Notification,
    base_url: &str,
) -> Result<(), anyhow::Error> {
    let client = reqwest::Client::new();

    tracing::debug!(
        "notification json: {}",
        chatops_backend
            .notification_message(notification)?
            .to_string()
    );

    let res = client
        .post(chatops_backend.notification_url(base_url))
        .header("Content-Type", "application/json")
        .body(
            chatops_backend
                .notification_message(notification)?
                .to_string(),
        )
        .send()
        .await?;

    match res.error_for_status() {
        Ok(_) => {
            println!("chatops notification sent ({})", chatops_backend.name());
            Ok(())
        }
        Err(e) => Err(anyhow::anyhow!(
            "chatops notification response error ({}): {}",
            chatops_backend.name(),
            e.status()
                .map_or_else(|| "<none>".to_string(), |s| s.to_string()),
        )),
    }
}

#[cfg(test)]
mod tests {
    use crate::app::chatops::{notify_with_base_url, ChatopsBackend, CommandData, Notification};
    use httpmock::Method::POST;
    use httpmock::MockServer;
    use serde_json::json;

    fn dummy_notification() -> Notification {
        Notification {
            sections: vec![],
            command: CommandData::Plan {
                file: "plan.json".to_string(),
                backend: Some("test".to_string()),
                backend_config: None,
            },
        }
    }

    #[tokio::test]
    async fn test_chatops_slack_notification() {
        let backend = ChatopsBackend::Slack {
            secret: "very_secret".to_string(),
        };
        let notification = dummy_notification();

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
        let notification = dummy_notification();

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
        let notification = dummy_notification();

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
    fn slack_message_format_is_blocks() {
        let backend = ChatopsBackend::Slack { secret: "s".into() };
        let msg = backend.notification_message(&dummy_notification()).unwrap();
        assert_eq!(
            msg,
            json!(
                        {
              "blocks": [
                {
                  "block_id": "approval_actions",
                  "elements": [
                    {
                      "action_id": "approve_button",
                      "style": "primary",
                      "text": {
                        "text": "Approve",
                        "type": "plain_text"
                      },
                      "type": "button",
                      "value": "{\"Plan\":{\"file\":\"plan.json\",\"backend\":\"test\",\"backend_config\":null}}"
                    },
                    {
                      "action_id": "deny_button",
                      "style": "danger",
                      "text": {
                        "text": "Deny",
                        "type": "plain_text"
                      },
                      "type": "button",
                      "value": "{\"Plan\":{\"file\":\"plan.json\",\"backend\":\"test\",\"backend_config\":null}}"
                    }
                  ],
                  "type": "actions"
                }
              ]
            })
        );
    }

    #[test]
    fn discord_message_format_is_content_field() {
        let backend = ChatopsBackend::Discord { token: "t".into() };
        let msg = backend.notification_message(&dummy_notification()).unwrap();
        assert_eq!(msg, json!({ "content": "" }));
    }
}
