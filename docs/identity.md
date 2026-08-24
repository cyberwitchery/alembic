# identity

identity in alembic is the `uid` alone. a uid names one logical object for its
whole life: it is assigned exactly once, and nothing ever recomputes it because
a key, a type, a backend, or the structure of a map spec changes. everything
else in this page follows from that law.

## uid vs key

- the `uid` is the object's identity: immutable, opaque, a uuid.
- the `key` is the object's locator: the human-readable natural key used to
  find the object's copy on a backend and to adopt existing backend objects.
  keys are mutable; renaming a key is an ordinary update.

one field cannot be both the immutable identity and the mutable natural key,
which is why the envelope carries both.

## logical identity vs materialization

`(type, uid)` names a materialization of the logical object: its copy in one
vocabulary, on one backend table. state binds materializations to backend rows.
the uid survives a type change; the materialization does not (see retype).

## how a uid is assigned

exactly one of four ways:

1. **authored**: you write it in an inventory. the file is the ledger.
2. **minted at first sight**: `import` meets a backend object no state binds
   and mints `uid_v5(type, key)` deterministically from its first-sight
   identity. the minted uid lands in the imported file; from then on it is
   carried, never recomputed.
3. **inherited**: a one-to-one `map` emit and `emit: passthrough` carry the
   source uid unchanged. the emitted object is the same logical object in
   another vocabulary.
4. **declared**: a multi-emit declares every emitted object's uid explicitly;
   a group emit's identity defaults to its group value.

`uid_v5(type, key)` is a minting convention, not a live identity function:
after the mint, key changes never touch the uid.

## the two ledgers

identity exists only where something records it:

- **inventory files** carry authored, minted, and inherited uids.
- **state** (see `docs/state.md`) binds each `(type, uid)` to one backend
  instance's row, and is scoped to exactly that backend instance.

an object reachable through neither ledger has no durable identity, only its
key. two places are inherently ledger-less and therefore value-identified:
aggregates grouped by a bare value, and stateless import.

## map

- a **single emit** with no `uid:` inherits the source uid, target-type change
  included. renaming a key through a map is an update, never a create+delete.
- an explicit `uid:` always wins: a template (`uid: "${uid}"`), a
  `v5: {type, stable}` expression, or the keyword `target`.
- **`uid: target`** mints value identity from the rendered target
  `(type, key)`: the deliberate identity break, for an emit that is a new
  object rather than a translation of its source.
- a **multi-emit** assigns identity explicitly on every emit, a one-element
  list included, so reshaping `emit:` into a list cannot silently change
  identity. `uid: "${uid}"` marks the continuing object of a split; siblings
  anchor on the source (`stable: "${uid}#mgmt"`) or their own defining value.
- **passthrough** is genuinely unchanged: key, attrs, and uid.
- **`group_by`** emits keep value identity: the aggregate is its group value,
  membership changes move nothing, and renaming the group value is a
  re-creation. grouping by a *ref* anchors the aggregate on the referenced
  object's uid, which survives the referent's renames; that is the pattern for
  aggregates that deserve durable identity.
- rule names, emit order, and file layout are never identity-bearing. the only
  identity-bearing text in a spec is the `uid:` expression itself.

## import

import assigns identity state-first: a backend object state already binds
keeps its uid, whatever its key says now, so backend-side renames round-trip
as the same logical object. an object state has never met is minted from its
first-sight `(type, key)`. refs resolve in the same uid space.

`import --stateless` drops the memory deliberately: every uid is minted from
the observed `(type, key)`, so a rename reads as a new object. use it for
one-shot audits and reproducible snapshots, not as the normal mode.

## adoption and supersession

when no state mapping answers for a declared object, `plan` adopts the backend
object sharing its key: it binds the declared uid to that backend row. adoption
writes identity memory that later authorizes updates and deletes, so it is
never silent: every run reports `adopted N existing object(s) by key` with the
bindings, and a binding that displaces another uid is reported as superseded.
`plan --no-adopt` disables key adoption for first contact with a populated
backend: state-known objects still match, everything else plans as a create.
it conflicts with `--allow-delete`: refusing to identify a backend object by
key is refusing to know enough to replace it.

## retype

declaring an object under a new type with the same uid re-materializes it: the
logical object survives, the backend copy cannot (a type is a table). the plan
renders the pair as a retype:

```
retype (create the new materialization, then delete the old):
  dcim.site -> location.site {"slug":"fra1"}
```

apply is not atomic: the create lands first, then the delete, and both
materializations coexist in between. a run interrupted after the create
resumes by re-issuing the delete. if the backend cannot host both at once (a
uniqueness rule spanning the two types), the create fails and nothing is
deleted; converge in two runs.

## designing keys

keys locate and adopt, so design them the way the backend scopes uniqueness.
an interface named `eth0` exists on every device, so a name-only key collides
as soon as a second device appears; the identity-relevant locator is
`(device, name)`:

```yaml
dcim.interface:
  key:
    device: {type: ref, target: dcim.device}
    name: {type: string}
```

a ref-typed key field holds the target's uid, and the engine resolves
ref-keyed identity within a single read. renaming the parent changes no uid
and cascades nothing.
