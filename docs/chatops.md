# chatops

this feature makes the alembic cli send notification to your chat
service of choice after running certain commands.

configured via the key `chatops_backend`. example:

```yaml
chatops_backend:
  Slack:
    secret: "XXXXXXXX/YYYYYYYY/ZZZZZZZZZZZZZZZZ"
```

notifications will be sent when the following cli commands are run:

- `plan` (after successfully writing the plan file; not on `--dry-run`
  or `--report`). this will allow the chat user to approve the plan,
  which will do a remote call to run `apply` on the machine set up to
  run the `alembic-chatops` tool (found in alembic-ops). by default
  this must be the same host as where the original `alembic plan`
  command was executed (use `ALEMBIC_MACHINE_ID_OVERRIDE` to work
  around this).

available backends are:

# Slack

make sure that your organization has created a dedicated alembic slack
app with adequate privileges before you try to set up the chatops
feature.

required configuration keys:

- `secret` (env var `ALEMBIC_SLACK_SECRET`) which is a string in the format
  "XXXXXXXX/YYYYYYYY/ZZZZZZZZZZZZZZZZ". it can be found under "Incoming
  Webhooks" in the settings page for your slack app.
