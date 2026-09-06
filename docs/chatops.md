# chatops

configured via the key `chatops_backend`. example:

```yaml
chatops_backend:
  Slack:
    secret: "XXXXXXXX/YYYYYYYY/ZZZZZZZZZZZZZZZZ"
```

available backends are:

- `Slack`
    - `secret`
- `Discord`
    - `token`

notifications will be sent when the following cli commands are run:

- `plan` (after successfully writing the plan file; not on `--dry-run` or `--report`)
