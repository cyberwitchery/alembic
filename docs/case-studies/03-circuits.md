# case study: circuits

## goal

provision a provider, circuit type, circuit, and termination.

## inventory

```yaml
objects:
  - uid: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    type: circuits.provider
    key: "slug=acme-telco"
    attrs:
      name: "acme telco"
      slug: "acme-telco"

  - uid: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    type: circuits.circuit_type
    key: "slug=dia"
    attrs:
      name: "dia"
      slug: "dia"

  - uid: "cccccccc-cccc-cccc-cccc-cccccccccccc"
    type: circuits.circuit
    key: "cid=ACME-001"
    attrs:
      cid: "ACME-001"
      provider: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
      type: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
      status: "active"

  - uid: "dddddddd-dddd-dddd-dddd-dddddddddddd"
    type: circuits.circuit_termination
    key: "circuit=ACME-001"
    attrs:
      circuit: "cccccccc-cccc-cccc-cccc-cccccccccccc"
      term_side: "A"
```

## commands

```bash
alembic plan -f /path/to/circuits.yaml -o /tmp/plan.json \
  --netbox-url http://localhost:8000 --netbox-token $NETBOX_TOKEN

alembic apply -p /tmp/plan.json \
  --netbox-url http://localhost:8000 --netbox-token $NETBOX_TOKEN --allow-delete
```

## notes

- circuit terminations may require additional fields depending on netbox version.
