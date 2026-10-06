# changelog.d

one file per changelog entry, holding the single line that goes into
`CHANGELOG.md`: a bullet, `- <subject>: <what changed> (#<issue>)`. name it
after the issue or the change (`477-reference-cycles.md`), so two prs never
write to the same file. `CLAUDE.md` says what earns an entry.

`scripts/changelog.sh check` validates the fragments in ci.
`scripts/changelog.sh release <version>` moves them into `CHANGELOG.md` under
a new `## [<version>] - <date>` heading at release time and deletes them.
