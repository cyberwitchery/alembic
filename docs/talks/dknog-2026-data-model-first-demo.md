# dknog 2026: 10-minute intro + live demo

event context: data-driven network automation and creating data models.

goal: show model-first automation with alembic, including an entity not universally available across systems (`virt.hypervisor` with child `virt.vm`).

## narrative in one sentence

define your own schema once, keep references stable by UID, and let alembic generate deterministic plans and converge supported backends.

## run-of-show (10 minutes)

### 0:00-1:30 problem framing

- most automation is backend-schema-first and hard to port.
- alembic is model-first: your schema + objects are the source of truth.
- backend adapters are execution targets, not the modeling boundary.

### 1:30-3:00 show base model

open `examples/talks/dknog-2026/core-v1.yaml`.

talk track:

- keys are human-stable identity per type.
- attrs are desired state.
- references are UID-based, not backend-ID-based.

command:

```bash
cargo run -p alembic-cli -- validate -f examples/talks/dknog-2026/core-v1.yaml
```

### 3:00-5:30 plan and apply

command:

```bash
cargo run -p alembic-cli -- plan -f examples/talks/dknog-2026/core-v1.yaml -o /tmp/dknog-plan-v1.json
```

show plan file quickly:

```bash
jq '.ops | map(.type) | group_by(.) | map({op: .[0], count: length})' /tmp/dknog-plan-v1.json
```

apply (interactive for audience trust):

```bash
cargo run -p alembic-cli -- apply -p /tmp/dknog-plan-v1.json --interactive
```

### 5:30-7:30 evolve desired model and re-plan

switch to `examples/talks/dknog-2026/core-v2.yaml` (adds `leaf02`, changes `leaf01` status).

```bash
cargo run -p alembic-cli -- plan -f examples/talks/dknog-2026/core-v2.yaml -o /tmp/dknog-plan-v2.json
jq '.ops | map(.type) | group_by(.) | map({op: .[0], count: length})' /tmp/dknog-plan-v2.json
```

point: this is deterministic intent-diff from model deltas.

### 7:30-9:00 custom entity not present everywhere

open `examples/talks/dknog-2026/extended-model.yaml`.

highlight:

- `virt.hypervisor`
- `virt.vm` referencing hypervisor
- no tool-specific plugin required to define this model

validate:

```bash
cargo run -p alembic-cli -- validate -f examples/talks/dknog-2026/extended-model.yaml
```

point: modeling is not constrained by a single backend's native object catalog.

### 9:00-10:00 close

- start with 2-3 types and one workflow.
- grow your schema as your network intent matures.
- adapters are implementation details; data model is product code.

## command checklist (pre-stage)

```bash
cargo build -p alembic-cli
rm -f /tmp/dknog-plan-v1.json /tmp/dknog-plan-v2.json
```

optional diagnostics:

```bash
export RUST_LOG=warn
```

## fallback if live backend is unavailable

- run only `validate` + `plan` commands.
- still show deterministic operation output from plan json.
- keep `extended-model.yaml` as the custom-model centerpiece.
