# agents

alembic is usable from an agent with a shell today: the command set is small,
validation errors and drift reports have json forms, plans are deterministic
artifacts, and only `apply` writes objects. what an agent cannot read off the
command line is the operational contract around those commands, which lives
across `docs/cli.md`, `docs/engine.md`, `docs/identity.md` and `docs/state.md`.

the `alembic` skill collects that contract as instructions: what each command
touches, the invariants a plausible-looking run can violate, which task takes
which pipeline, and five worked workflows. these pages are authoritative where
the two disagree -- the skill is an operating guide over the docs, not a second
source of truth.

it ships inside the binary. `crates/alembic-cli/skills/alembic/SKILL.md` is the
file embedded at compile time. `.agents/skills/alembic` points at the same
directory for standard project discovery, and `.claude/skills/alembic` provides
the Claude-specific compatibility path. a checkout therefore reads the copy it
publishes without making either host layout canonical.

## what it states

- relationships hold alembic uids, never backend ids
- a uid is assigned once: an edit or a rename carries it, and recomputing it
  creates a different object
- removing an attr stops managing it; setting it to `null` clears it
- an update writes the full declared projection, not only the plan's `changes`
- `plan --report` is read-only only without `--provision`
- planning can persist identity adoption unless `--report` or `--dry-run`
- `--allow-delete` asserts the inventory is complete inside its top-level
  `scope:`; without one it covers every declared type on the backend. it is not a
  retry flag
- an interrupted apply resumes from its journal by re-running the same plan

it also separates read-only inspection from local artifact writes and from
backend writes, requires validation after an inventory or map edit and a reviewed
plan before an apply, and keeps credentials in the environment rather than in a
committed backend config.

## installing it

the binary writes it out (`docs/cli.md`, skill):

```bash
alembic skill install alembic
alembic skill install alembic --dir /srv/intent/.agents/skills
```

`--dir` is a skills root: the file lands at `<dir>/<name>/SKILL.md`. it defaults
to `.agents/skills`; pass a host-specific root when needed. for a host that reads
something else entirely, `alembic skill show alembic` prints the same markdown
to stdout.

installing needs no network, no backend and no state. a generated ownership and
content marker lets a later Alembic replace an unchanged installed copy; a local
edit or any unowned file is refused unless `--force` is explicit. the write is a
temporary file renamed into place, so an interruption cannot truncate the skill.

that is the point of embedding the text rather than publishing it somewhere to
be fetched: a skill states what the command surface does not, so one that
describes a different build states it wrongly, and confidently. a release copy
pins documentation to its version tag. an unreleased build from a Git checkout
pins it to that commit, or to `main` when the checkout is dirty, and identifies
itself as unreleased in the stamp.

re-run `alembic skill install` after upgrading alembic.

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

nor is `skill install` a plugin manifest. a claude code marketplace entry would
install and update natively for that audience, and can carry the same file, but
it tracks a branch rather than the binary an operator is running, which is the
version the contract has to match.
