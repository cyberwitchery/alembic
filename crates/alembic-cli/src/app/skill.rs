//! agent skills, embedded in the binary.
//!
//! a skill states the operational contract around the commands: the identity
//! law, what stops managing a field, which flags are assertions rather than
//! switches. it is worth nothing when it describes a different alembic than the
//! one it is installed beside, so the text ships inside the binary rather than
//! being fetched, and every copy `install` writes says which version wrote it.

use anyhow::{anyhow, Result};
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

/// where the skills root goes when `install` is not told one. the layout agent
/// hosts read is `<root>/<name>/SKILL.md`.
pub(crate) const DEFAULT_SKILLS_DIR: &str = ".agents/skills";

/// the version this binary is, which is the version its skills describe.
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// absent for a packaged release or a clean checkout of its version tag. a
/// source build carries the exact commit, or `main` when uncommitted changes
/// mean no commit describes the binary.
const SOURCE_REF: Option<&str> = option_env!("ALEMBIC_SOURCE_REF");

/// one embedded skill: the name it installs under, a line for `list`, and the
/// markdown itself.
#[derive(Debug)]
struct Skill {
    name: &'static str,
    summary: &'static str,
    source: &'static str,
}

/// the `.agents` and `.claude` project paths point at this file, so every host
/// and an install carry the same text.
const SKILLS: &[Skill] = &[Skill {
    name: "alembic",
    summary: "driving the cli: what each command touches, the invariants, worked workflows",
    source: include_str!("../../skills/alembic/SKILL.md"),
}];

fn find(name: &str) -> Result<&'static Skill> {
    SKILLS
        .iter()
        .find(|skill| skill.name == name)
        .ok_or_else(|| {
            let names = SKILLS
                .iter()
                .map(|skill| skill.name)
                .collect::<Vec<_>>()
                .join(", ");
            anyhow!("no skill named `{name}`; this binary carries: {names}")
        })
}

fn fingerprint(content: &str) -> u64 {
    content.bytes().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    })
}

fn stamp(skill: &Skill, content: &str) -> String {
    format!(
        "{content}<!-- alembic-skill-install name=\"{}\" content=\"{:016x}\" -->\n",
        skill.name,
        fingerprint(content)
    )
}

/// the skill as it leaves the binary.
///
/// release builds link to their version tag. a Git build links to the source
/// commit, or to `main` when the checkout was dirty and no commit describes it.
fn render(skill: &Skill) -> String {
    let docs_ref = SOURCE_REF
        .map(str::to_string)
        .unwrap_or_else(|| format!("v{VERSION}"));
    let pinned = skill
        .source
        .replace("/blob/main/", &format!("/blob/{docs_ref}/"));
    let provenance = match SOURCE_REF {
        Some(source_ref) => format!("unreleased alembic {VERSION} source {source_ref}"),
        None => format!("alembic {VERSION}"),
    };
    let content = format!(
        "{}\n---\n\ninstalled from {provenance} by `alembic skill install`, and it describes \
         that build's cli. re-run the command after upgrading alembic: a skill ahead of its \
         binary states the contract of a cli you are not running.\n",
        pinned.trim_end()
    );
    stamp(skill, &content)
}

/// `alembic skill list`.
pub(crate) fn list() {
    for skill in SKILLS {
        println!("{}\t{}", skill.name, skill.summary);
    }
}

/// `alembic skill show <name>`, for a host that reads something other than a
/// skills directory.
pub(crate) fn show(name: &str) -> Result<()> {
    print!("{}", render(find(name)?));
    Ok(())
}

fn is_unchanged_install(skill: &Skill, content: &str) -> bool {
    let marker = format!(
        "<!-- alembic-skill-install name=\"{}\" content=\"",
        skill.name
    );
    let Some(start) = content.rfind(&marker) else {
        return false;
    };
    let Some(encoded) = content[start + marker.len()..].strip_suffix("\" -->\n") else {
        return false;
    };
    u64::from_str_radix(encoded, 16).ok() == Some(fingerprint(&content[..start]))
}

fn write_atomic(path: &Path, content: &str) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("{} has no parent directory", path.display()))?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("SKILL.md");
    for attempt in 0..100 {
        let temporary = parent.join(format!(".{name}.tmp-{}-{attempt}", std::process::id()));
        let mut file = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(anyhow!(
                    "could not create temporary file beside {}: {err}",
                    path.display()
                ))
            }
        };
        let result = file
            .write_all(content.as_bytes())
            .and_then(|()| file.sync_all())
            .and_then(|()| fs::rename(&temporary, path));
        if let Err(err) = result {
            // the original destination is intact; cleanup is best-effort because
            // the write error remains the useful failure to return.
            let _ = fs::remove_file(&temporary);
            return Err(anyhow!(
                "could not write {} atomically: {err}",
                path.display()
            ));
        }
        return Ok(());
    }
    Err(anyhow!(
        "could not reserve a temporary file beside {}",
        path.display()
    ))
}

/// `alembic skill install <name> --dir <root>`, writing `<root>/<name>/SKILL.md`.
pub(crate) fn install(name: &str, dir: &Path, force: bool) -> Result<PathBuf> {
    let skill = find(name)?;
    let target = dir.join(skill.name);
    fs::create_dir_all(&target)
        .map_err(|err| anyhow!("could not create {}: {err}", target.display()))?;
    let path = target.join("SKILL.md");
    if path.exists() && !force {
        let existing = fs::read_to_string(&path)
            .map_err(|err| anyhow!("could not inspect existing {}: {err}", path.display()))?;
        if !is_unchanged_install(skill, &existing) {
            return Err(anyhow!(
                "refusing to replace {}: it is not an unchanged skill installed by alembic; \
                 pass --force to replace it",
                path.display()
            ));
        }
    }
    write_atomic(&path, &render(skill))?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn every_skill_carries_the_frontmatter_a_host_reads() {
        for skill in SKILLS {
            let rendered = render(skill);
            assert!(
                rendered.starts_with("---\nname: "),
                "{} must open with skill frontmatter",
                skill.name
            );
            assert!(
                rendered.contains("\ndescription: "),
                "{} must declare a description",
                skill.name
            );
        }
    }

    #[test]
    fn rendering_pins_the_doc_links_to_this_build() {
        let rendered = render(find("alembic").unwrap());
        let docs_ref = SOURCE_REF
            .map(str::to_string)
            .unwrap_or_else(|| format!("v{VERSION}"));
        assert!(
            rendered.contains(&format!("/blob/{docs_ref}/docs/identity.md")),
            "the doc links must name the source that built the file"
        );
    }

    #[test]
    fn rendering_records_the_version_it_came_from() {
        let rendered = render(find("alembic").unwrap());
        assert!(rendered.contains(&format!("alembic {VERSION}")));
        if SOURCE_REF.is_some() {
            assert!(rendered.contains("unreleased alembic"));
        }
        assert!(is_unchanged_install(find("alembic").unwrap(), &rendered));
        assert!(rendered.ends_with('\n'));
    }

    #[test]
    fn an_unknown_skill_names_what_is_embedded() {
        let err = find("nautobot").unwrap_err().to_string();
        assert!(err.contains("no skill named `nautobot`"), "{err}");
        assert!(err.contains("alembic"), "{err}");
    }

    #[test]
    fn install_writes_the_layout_a_host_reads() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("nested/skills");
        let path = install("alembic", &root, false).unwrap();
        assert_eq!(path, root.join("alembic/SKILL.md"));
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            render(find("alembic").unwrap())
        );
    }

    #[test]
    fn installing_again_replaces_a_stale_copy() {
        let dir = tempdir().unwrap();
        let skill = find("alembic").unwrap();
        let target = dir.path().join("alembic");
        fs::create_dir(&target).unwrap();
        let path = target.join("SKILL.md");
        fs::write(
            &path,
            stamp(skill, "an older skill, describing an older cli\n"),
        )
        .unwrap();
        let again = install("alembic", dir.path(), false).unwrap();
        assert_eq!(again, path);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            render(find("alembic").unwrap()),
            "installing over a stale copy is the upgrade path"
        );
    }

    #[test]
    fn install_refuses_an_unowned_existing_skill() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("alembic");
        std::fs::create_dir(&target).unwrap();
        let path = target.join("SKILL.md");
        std::fs::write(&path, "a skill owned by somebody else\n").unwrap();

        let err = install("alembic", dir.path(), false)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("not an unchanged skill installed by alembic"),
            "{err}"
        );
        assert_eq!(
            std::fs::read_to_string(path).unwrap(),
            "a skill owned by somebody else\n"
        );
    }

    #[test]
    fn install_refuses_a_locally_modified_alembic_skill() {
        let dir = tempdir().unwrap();
        let path = install("alembic", dir.path(), false).unwrap();
        let mut modified = fs::read_to_string(&path).unwrap();
        modified.insert_str(0, "locally adjusted\n");
        fs::write(&path, &modified).unwrap();

        let err = install("alembic", dir.path(), false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("pass --force"), "{err}");
        assert_eq!(fs::read_to_string(path).unwrap(), modified);
    }

    #[test]
    fn force_replaces_an_unowned_skill() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("alembic");
        fs::create_dir(&target).unwrap();
        let path = target.join("SKILL.md");
        fs::write(&path, "a skill owned by somebody else\n").unwrap();

        install("alembic", dir.path(), true).unwrap();
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            render(find("alembic").unwrap())
        );
        assert_eq!(fs::read_dir(target).unwrap().count(), 1);
    }

    #[test]
    fn install_reports_an_unwritable_directory_rather_than_the_path_it_wanted() {
        let dir = tempdir().unwrap();
        let blocked = dir.path().join("skills");
        std::fs::write(&blocked, "not a directory").unwrap();
        let err = install("alembic", &blocked, false).unwrap_err().to_string();
        assert!(err.contains("could not create"), "{err}");
    }
}
