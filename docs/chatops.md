# chatops

Configured via the key `chatops_backend`. Example:

```yaml
chatops_backend:
  Slack:
    secret: "XXXXXXXX/YYYYYYYY/ZZZZZZZZZZZZZZZZ"
```

Available backends are:

- `Slack`
    - `secret`
- `Discord`
    - `token`

Notifications will be sent on the following events:

- `plan` (after successfully writing the plan file; not on `--dry-run` or `--report`)
