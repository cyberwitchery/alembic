---
name: alembic
description: Operate the alembic CLI (validate, import, map, plan, apply) against a DCIM/IPAM backend such as netbox, nautobot, infrahub or a generic REST service. Use when a task involves an alembic inventory, map spec, plan file or state store, when converging declared intent onto such a backend, or when reading drift out of one. Carries the identity and safety rules the command line does not state.
---

# alembic

alembic converges a declared inventory onto a backend. five commands: `validate`,
`import`, `map`, `plan`, `apply`. only `apply` writes objects.

the command surface is small and its artifacts are json, so it is usable from a
shell without an integration. what the surface does not state is the contract
below: the identity law, what "stop managing" means, and which flags are
assertions rather than switches. get one of those wrong and a run that looks
successful is wrong in a way no exit code reports.

the canonical documentation is authoritative wherever this file is thinner or
disagrees. it is `docs/` in a checkout of the alembic repository, and the links
below otherwise:

| topic | doc |
| --- | --- |
| commands, flags, json shapes | [`docs/cli.md`](https://github.com/cyberwitchery/alembic/blob/main/docs/cli.md) |
| uids, keys, adoption, retype | [`docs/identity.md`](https://github.com/cyberwitchery/alembic/blob/main/docs/identity.md) |
| validation, diff rules, import | [`docs/engine.md`](https://github.com/cyberwitchery/alembic/blob/main/docs/engine.md) |
| identity memory and its locking | [`docs/state.md`](https://github.com/cyberwitchery/alembic/blob/main/docs/state.md) |
| inventory format, field schemas | [`docs/inventory.md`](https://github.com/cyberwitchery/alembic/blob/main/docs/inventory.md), [`docs/ir.md`](https://github.com/cyberwitchery/alembic/blob/main/docs/ir.md) |
| plan file shape | [`docs/plan.md`](https://github.com/cyberwitchery/alembic/blob/main/docs/plan.md) |

## what each command touches

read this before choosing a command, and say which column you are in before you
run it.

| invocation | reads backend | local output | writes backend | loads state | state lock | saves state |
| --- | --- | --- | --- | --- | --- | --- |
| `validate -f` | no | `-o` report, if given | no | no | none | no |
| `map -f --spec -o` | no | `-o` inventory | no | no | none | no |
| `map transform --spec` | no | stdout | no | no | none | no |
| `import -f -o` | yes | `-o` inventory | no | yes | shared | no |
| `import -f -o --stateless` | yes | `-o` inventory | no | no | none | no |
| `plan --report` | yes | `-o` report, if given | no | yes | shared | no |
| `plan --dry-run` | yes | stdout | no | yes | shared | no |
| `plan --report --provision` | yes | `-o` report, if given | **schema** | yes | exclusive | no |
| `plan -o` | yes | plan | no | yes | exclusive | yes |
| `plan -o --provision` | yes | plan | **schema** | yes | exclusive | yes |
| `apply -p` | yes | journal, `-o` report | **objects and schema** | yes | exclusive | yes |

state loading, locking, and saving are separate properties. a shared lock means
the run may load identity memory but cannot persist changes to it. `validate` and
`map` do not touch state at all; stateless import deliberately does not. adding
`--provision` to `plan --report` changes its lock to exclusive because the run
writes backend schema, but the report path still does not save identity state.
`--dry-run --provision` is rejected at argument parsing.

## invariants

these hold whatever the task is.

1. **relationships hold uids.** a `ref` or `list_ref` attr names the target
   object's alembic uid, never the backend's own id. a backend id in a ref fails
   validation, and an observation that reports one is refused at plan time. never
   put a backend id in an inventory, in a key or in `attrs`.
2. **a uid is assigned once.** editing an object, renaming its key, or moving it
   between vocabularies carries the existing uid unchanged. recomputing a uid
   from the new key produces a *different logical object*: alembic plans a create
   beside a delete, not a rename. when you edit an object, copy its uid across
   verbatim; when a map spec emits it, let the emit inherit the source uid.
3. **removing an attr is not clearing it.** diffing is additive-only: only fields
   the inventory declares are compared, and a field you delete from `attrs` is
   simply no longer managed, leaving the backend's value in place. to clear a
   field, declare it `nullable: true` in the schema and set it to `null`. "absent"
   and "null" are different instructions; never swap one for the other.
4. **an update writes the full declared projection.** apply sends every declared
   field of an updated object, not only the fields the plan listed under
   `changes`. `changes` is the plan-time diff, recorded for review; a backend edit
   made after the plan was written is converged back. approving an update means
   approving that whole write.
5. **`plan --report` is read-only only without `--provision`.** `--provision`
   runs the adapter's `ensure_schema` against the backend before the report is
   computed, which issues schema writes. `--report --provision` is a writing run
   wearing a report's name. for a read-only look, `--report` alone.
6. **planning can persist identity.** when no state mapping answers for a
   declared object, `plan` adopts the backend object sharing its key and saves
   that binding, which later authorizes updates and deletes of that row. the run
   reports it (`adopted N existing object(s) by key`, plus any `superseded:`
   line). only `--report` and `--dry-run` save nothing. read the adoption lines
   before treating a plan run as a look.
7. **`--allow-delete` is an assertion, not a retry flag.** it says the inventory
   is complete inside its top-level `scope:` (`docs/inventory.md`): only observed
   objects matching a scope entry are delete candidates, and a type with no entry
   carries no delete authority. without `scope:`, the historical assertion covers
   every declared type on the backend, including objects another inventory
   manages. pass it only when the operator has confirmed that completeness claim
   and has read the deletes in the plan.
8. **an interrupted apply resumes from its journal.** apply appends each completed
   create/update to a journal under `.alembic/`, keyed to the backend and to the
   plan's operations. re-running the same `alembic apply` picks it up by name and
   skips what it records. never hand-edit or delete a journal, never re-plan
   around one, and never treat its presence as debris: it is the record of what
   already landed on a backend that has no rollback.

further rules worth holding: state is scoped to one backend instance and refuses
any other; deleting the state file forgets identity, so objects renamed in the
meantime come back as create+delete; a key naming several backend objects is data
until a run must dereference it, and then it fails rather than picking one.

## routing a task

work in this order, and stop at the first line whose output answers the task.

- **"what is on the backend?" / "what drifted?"** → `plan --report` (add `-o` for
  the json). no plan file, no state written. `import -o` when you want the
  observed state as an inventory rather than a diff.
- **"add / change / remove an object"** → edit the inventory → `validate` →
  `plan -o` → read the printed summary → `apply -p`.
- **"the source data is in another vocabulary"** → `map -f --spec -o` → `validate`
  the output → then the plan/apply path.
- **"why does this transform emit that?"** → `map transform --spec <name> <json>`,
  which needs no inventory and no backend.
- **"the apply died partway"** → re-run the same `alembic apply -p <same plan>`.

two rules bracket that pipeline:

- **validate after every edit.** any change to an inventory or a map spec is
  followed by `alembic validate -f <inventory>` before anything reads a backend.
  a map's output is an inventory: validate it too.
- **a plan is reviewed before it is applied.** `plan` prints a per-op summary and
  writes the json; apply exactly that file. never pipe a plan straight into an
  apply the operator has not seen, and re-plan rather than editing a plan file by
  hand.

## flags you do not reach for to make a run pass

a failing run is telling you something about the intent. these three flags change
what the run *asserts*, so adding one to get past an error converts a question
into a silent answer. none of them belongs in a retry:

- `--allow-delete` — asserts the inventory is complete (invariant 7). an apply
  that refuses a plan's deletes, or a backend still holding objects the inventory
  dropped, is answered either by the operator confirming those exact objects
  should go or by leaving them alone. `plan --report` names them under `extra`
  without planning anything.
- `--provision` — writes backend schema. if a field is missing on the backend,
  say so; do not provision under a `--report` and call the run read-only.
- `--no-adopt` — refuses to identify backend objects by key, so everything
  unmatched plans as a create. it is the cautious mode for first contact with a
  populated backend, not a way to quiet an adoption you did not expect. an
  unexpected adoption means the keys are not what you thought.

if a run fails, report the failure and what it implies about the inventory. widening
authority is the operator's decision, not a workaround.

## credentials

credentials live in the environment: `NETBOX_TOKEN`, `NAUTOBOT_TOKEN`,
`INFRAHUB_TOKEN`, `PEERINGDB_API_KEY`, and whatever an external adapter reads
(`docs/cli.md`, environment variables). a backend config file is committed to a
repo; a token in one is a leaked token. use `--backend <name>` with the
environment, or a `--backend-config` that names the endpoint and leaves the
secret out. never echo a token into a log, a report, or a commit.

## worked workflows

### inspect drift without writing

```bash
alembic plan --backend netbox -f inventory.yaml --report -o drift.json
```

reads the backend, writes no plan, saves no state. the report groups into
`changed` (declared, present, diverging), `missing` (declared, absent), and
`extra` (present, undeclared — populated whether or not `--allow-delete` was
passed). three empty lists mean no drift. do not add `--provision` (invariant 5).

### add an object

```bash
# 1. edit inventory.yaml: a new object with a fresh uid you generate once
# 2.
alembic validate -f inventory.yaml
# 3.
alembic plan --backend netbox -f inventory.yaml -o plan.json
# 4. read the summary: the new object is a create, and nothing else moved
# 5.
alembic apply -p plan.json --backend netbox
```

mint the uid yourself (any uuid) and write it into the file; it is the object's
identity from then on. a ref to it names that uid.

### rename while preserving identity

```bash
# edit the object's key in place, leaving `uid:` exactly as it was
alembic validate -f inventory.yaml
alembic plan --backend netbox -f inventory.yaml -o plan.json
```

the plan must show **one update**. a create beside a delete means the uid moved:
you recomputed it rather than carrying it, and applying that plan would destroy
and re-make the object rather than rename it. fix the inventory and re-plan.

a type change is the exception: the same uid under a new type is a `retype`,
which the plan renders as a create of the new materialization followed by a
delete of the old one. that is the logical object surviving, not identity moving.

### produce a plan for review

```bash
alembic plan --backend netbox -f inventory.yaml -o plan.json
```

hand over `plan.json` and the printed summary together, and say which of the
adoption lines the run reported. the plan is deterministic for a given inventory
and observation, so it re-reads the same later. remember that an update in it will
write every declared field of its object, not only the listed `changes`
(invariant 4). `--dry-run` prints the same plan to stdout without writing a file
or saving state, when the review does not need an artifact.

### resume an interrupted apply

```bash
# the interrupted run said:
#   WARN apply stopped after 3 of 12 create/update operations; the journal at
#   ./.alembic/netbox_journal_<hash>.yaml records what was applied, and
#   re-running the same plan resumes from there
alembic apply -p plan.json --backend netbox
```

same command, same plan file. no flag, no journal argument. the run skips what the
journal records and reports `applied N operations (after resuming, had previously
applied M operations)`; the journal is deleted once the plan applies in full, so
its presence means an apply is unfinished. do not re-plan first: a fresh plan is
keyed to different operations, starts a new journal, and leaves the old one
stranded. diagnose the original failure before re-running, since resume repeats
the operation that failed.

## exercises

[`fixtures/agent/`](https://github.com/cyberwitchery/alembic/blob/main/fixtures/agent)
in the alembic repository holds a file-backed backend and the inventories that
put the invariants above under test;
[`scripts/agent_fixtures.sh`](https://github.com/cyberwitchery/alembic/blob/main/scripts/agent_fixtures.sh)
runs them offline and checks the artifacts. run it when you change this file, and
read the exercises when an invariant here is unclear: each one is the smallest
case that distinguishes getting it right from getting it plausibly wrong.
