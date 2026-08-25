//! agent skills, embedded in the binary.
//!
//! a skill states the operational contract around the commands: the identity
//! law, what stops managing a field, which flags are assertions rather than
//! switches. it is worth nothing when it describes a different alembic than the
//! one it is installed beside, so the text ships inside the binary rather than
//! being fetched, and every copy `install` writes says which version wrote it.

use anyhow::{anyhow, Result};
use std::path::{Path, PathBuf};

/// where the skills root goes when `install` is not told one. the layout agent
/// hosts read is `<root>/<name>/SKILL.md`.
pub(crate) const DEFAULT_SKILLS_DIR: &str = ".claude/skills";

/// the version this binary is, which is the version its skills describe.
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// one embedded skill: the name it installs under, a line for `list`, and the
/// markdown itself.
#[derive(Debug)]
struct Skill {
    name: &'static str,
    summary: &'static str,
    source: &'static str,
}

/// the file is the same one `.claude/skills/alembic` points at, so a checkout
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

/// the skill as it leaves the binary.
///
/// two things happen to the source on the way out. the documentation links are
/// pinned to this binary's version, so an installed copy reads the pages for the
/// cli it describes rather than whatever `main` says by then; and a trailing
/// line records the version, since a skill's failure mode is being a release
/// ahead of the binary and saying so silently.
fn render(skill: &Skill) -> String {
    let pinned = skill
        .source
        .replace("/blob/main/", &format!("/blob/v{VERSION}/"));
    format!(
        "{}\n---\n\ninstalled from alembic {VERSION} by `alembic skill install`, and it \
         describes that version's cli. re-run the command after upgrading alembic: a skill \
         a release ahead of its binary states the contract of a cli you are not running.\n",
        pinned.trim_end()
    )
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

/// `alembic skill install <name> --dir <root>`, writing `<root>/<name>/SKILL.md`.
///
/// an existing file is overwritten: replacing a stale copy is what installing
/// again is for.
pub(crate) fn install(name: &str, dir: &Path) -> Result<PathBuf> {
    let skill = find(name)?;
    let target = dir.join(skill.name);
    std::fs::create_dir_all(&target)
        .map_err(|err| anyhow!("could not create {}: {err}", target.display()))?;
    let path = target.join("SKILL.md");
    std::fs::write(&path, render(skill))
        .map_err(|err| anyhow!("could not write {}: {err}", path.display()))?;
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
    fn rendering_pins_the_doc_links_to_this_version() {
        let rendered = render(find("alembic").unwrap());
        assert!(
            rendered.contains(&format!("/blob/v{VERSION}/docs/identity.md")),
            "the doc links must name the version that wrote the file"
        );
        assert!(
            !rendered.contains("/blob/main/"),
            "no link may still point at main: {rendered}"
        );
    }

    #[test]
    fn rendering_records_the_version_it_came_from() {
        let rendered = render(find("alembic").unwrap());
        assert!(rendered.contains(&format!("installed from alembic {VERSION}")));
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
        let path = install("alembic", &root).unwrap();
        assert_eq!(path, root.join("alembic/SKILL.md"));
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            render(find("alembic").unwrap())
        );
    }

    #[test]
    fn installing_again_replaces_a_stale_copy() {
        let dir = tempdir().unwrap();
        let path = install("alembic", dir.path()).unwrap();
        std::fs::write(&path, "an older skill, describing an older cli\n").unwrap();
        let again = install("alembic", dir.path()).unwrap();
        assert_eq!(again, path);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            render(find("alembic").unwrap()),
            "installing over a stale copy is the upgrade path"
        );
    }

    #[test]
    fn install_reports_an_unwritable_directory_rather_than_the_path_it_wanted() {
        let dir = tempdir().unwrap();
        let blocked = dir.path().join("skills");
        std::fs::write(&blocked, "not a directory").unwrap();
        let err = install("alembic", &blocked).unwrap_err().to_string();
        assert!(err.contains("could not create"), "{err}");
    }
}
