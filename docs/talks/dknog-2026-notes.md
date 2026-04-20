# alembic demo — speaker notes

## slide 1: alembic demo (5s)

"let me show you what this looks like in practice."

marcus gave you a good picture of the idea, but we didn’t want to leave you
without at least a bit of practice. now, to be clear, this is very much a proof
of concept and prototype. don’t get married to the speicifc invocations i’m
showing you or the workflows. the code i produced works, and it’s kind of cool,
but it’s not a system i would necessarily endorse yet.

---

## slide 2: the model (45s)

- this is the core idea: you describe your infrastructure as a schema.
  types, keys, fields. that's it.
- keys are how alembic identifies objects across backends. a site is
  keyed by slug, a device by name.
- fields can be plain values or references to other types. alembic
  resolves these for you.
- this is vendor-neutral — nothing here says netbox or nautobot.

---

## slide 3: objects (30s)

- objects are instances of those types. each gets a stable UUID that
  you control.
- references between objects use those UUIDs, not backend IDs. so your
  model doesn't break when you switch backends or rebuild.
- this whole file is your source of truth. check it into git, review
  it in a PR, generate it from your CMDB — whatever fits your workflow.

---

## slide 4: validate (15s)

- first step: validate offline. no network, no credentials needed.
- checks that your schema is consistent, keys are unique, UIDs don't
  collide, and every reference points at something real.
- catches mistakes before they hit your backend.

---

## slide 5: plan (20s)

- plan compares your desired state against what's actually in the
  backend right now.
- deterministic: same input, same plan. you can diff plans in CI.
- the plan is a JSON file — you can inspect it, store it, pass it
  to apply separately.

---

## slide 6: apply (30s)

- apply executes the plan. interactive mode asks you per operation.
- each operation is one API call. if something fails, you know
  exactly where it stopped.
- state file tracks the mapping from your UIDs to backend IDs, so
  next time alembic knows what already exists.

---

## slide 7: evolve the model (20s)

- now the interesting part: your infrastructure changes.
- edit the YAML. change a status, add a device. same file, same format.
- no imperative scripts, no "add this then update that." you declare
  what you want and alembic figures out the diff.

---

## slide 8: re-plan (15s)

- same command, same workflow. alembic sees one thing to create, one
  to update. nothing else is touched.
- this is the convergence loop: declare, plan, apply, repeat.

---

## slide 9: custom types (30s)

- here's where it gets powerful. these types don't exist in netbox or
  nautobot out of the box.
- you define them in the same schema. no plugins, no code, no waiting
  for upstream.
- references work across custom and built-in types. a VM points at a
  hypervisor which points at a device which points at a site.

---

## slide 10: validate custom types (30s)

- this is the question you're thinking: "my backend doesn't have
  these types, so what now?"
- alembic has two answers. first: projection. if you add custom
  fields to your objects — say `model.fabric` on a device —
  projection maps those into backend custom fields automatically.
  `--projection-propose` even creates the custom fields in netbox
  for you.
- second: many "custom" types already exist in backends. netbox
  has virtualization endpoints. alembic discovers them and
  converges against them. same plan, same apply.
- the schema is yours. the backend adapts.
- one open question we are still adapting is the creation of
  actual custom object types. it’s trivial in netbox and nautobot,
  we just have to do the leg work.

---

## slide 11: cast (20s)

- and if you need a backend that doesn't exist yet — maybe
  something internal, something org-specific — cast scaffolds
  one from your schema. one command, full Django REST API.

---

## slide 12: cast — admin (10s)

- you get a Django admin out of the box. all your types are there,
  ready to browse and edit.

---

## slide 13: cast — API (10s)

- and a REST API with the DRF browsable interface. alembic can
  converge against this the same way it converges against netbox.

---

## slide 14: other backends (15s)

- same model file, different `-b` flag. netbox, nautobot, or any
  REST API via the generic adapter.
- you don't rewrite your model when you switch backends.

---

## slide 15: one model, many targets (30s)

- this is the takeaway. you focus on describing your world — your
  sites, your devices, your types. alembic handles the rest.
- your system fits your world. not the other way around.
