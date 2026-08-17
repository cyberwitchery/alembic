# chatops

Configured via the key `chatops_backend`. Example:

```yaml
chatops_backend:
  Slack:
    secret: "XXXXXXXX/YYYYYYYY/ZZZZZZZZZZZZZZZZ"
```

Available backends are:

- `Slack`

Notifications will be sent on the following events:

- `plan`
