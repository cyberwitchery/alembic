# agents

alembic is usable from an agent with a shell today: the command set is small,
validation errors and drift reports have json forms, plans are deterministic
artifacts, and only `apply` writes objects. what an agent cannot read off the
command line is the operational contract around those commands, which lives
across `docs/cli.md`, `docs/engine.md`, `docs/identity.md` and `docs/state.md`.

`.claude/skills/alembic/SKILL.md` collects that contract as instructions: what
each command touches, the invariants a plausible-looking run can violate, which
task takes which pipeline, and five worked workflows. this page is authoritative
where the two disagree -- the skill is an operating guide over these docs, not a
second source of truth.

## what it states

- relationships hold alembic uids, never backend ids
- a uid is assigned once: an edit or a rename carries it, and recomputing it
  creates a different object
- removing an attr stops managing it; setting it to `null` clears it
- an update writes the full declared projection, not only the plan's `changes`
- `plan --report` is read-only only without `--provision`
- planning can persist identity adoption unless `--report` or `--dry-run`
- `--allow-delete` asserts the inventory is complete over every declared type on
  the backend (#413), and is not a retry flag
- an interrupted apply resumes from its journal by re-running the same plan

it also separates read-only inspection from local artifact writes and from
backend writes, requires validation after an inventory or map edit and a reviewed
plan before an apply, and keeps credentials in the environment rather than in a
committed backend config.

## using it

in a repository whose agent host reads `.claude/skills/`, copy the directory:

```bash
cp -r path/to/alembic/.claude/skills/alembic .claude/skills/
```

for any other host, `SKILL.md` is a single self-contained markdown file: point
the agent at it, or paste it into whatever the host reads as project
instructions. it names doc paths relative to this repository, so keep the docs
reachable, or replace the paths with links to them.

## exercising it

instructions that only sound complete are not evidence, so the invariants are
checked against artifacts instead. `fixtures/agent/` holds a file-backed external
adapter (`docs/external-adapters.md`) standing in for a backend, plus one
inventory per invariant: each is the starting intent with a single deliberate
edit. `scripts/agent_fixtures.sh` runs them offline and asserts what the cli did
-- the plan's ops, the drift report's categories, the backend store, and the
adapter's method log:

```bash
./scripts/agent_fixtures.sh
```

it needs `python3` and a built cli; set `ALEMBIC` to run something other than
`cargo run -q -p alembic-cli --`. the exercises cover a uid carried through a
rename (and the same rename with a recomputed uid, which is a create beside a
delete), a ref holding a backend id failing validation, an absent attr against a
null one, an undeclared object surfacing as `extra` while its delete is refused,
and `--report --provision` reaching `ensure_schema` where `--report` alone does
not.

## scope

this is the cli made deliberate as an agent surface, not a protocol. an mcp
server would draw a different boundary -- centrally held credentials,
preconfigured backend aliases, restricted inventory roots, remote execution,
host-mediated approval of an exact plan -- and none of that is needed to find out
whether the agent-facing semantics are sufficient. a gap these exercises expose
in the cli's own machine-readable discovery belongs in a cli issue, not inside a
wrapper.
