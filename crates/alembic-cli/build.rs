use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn git(manifest_dir: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(manifest_dir)
        .args(args)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let Some(root) = git(&manifest_dir, &["rev-parse", "--show-toplevel"]).map(PathBuf::from)
    else {
        // crates.io and release source archives have no Git checkout. their
        // Cargo package version is the release tag the embedded skill describes.
        return;
    };
    let manifest_dir = manifest_dir.canonicalize().unwrap_or(manifest_dir);
    let root = root.canonicalize().unwrap_or(root);
    if manifest_dir != root && manifest_dir != root.join("crates/alembic-cli") {
        // `cargo package --verify` builds below target/package inside the outer
        // checkout. that source is a package, not the checkout Git happened to
        // find by walking through its parent directories.
        return;
    }

    if let Some(git_head) = git(&manifest_dir, &["rev-parse", "--git-path", "HEAD"]) {
        println!("cargo:rerun-if-changed={git_head}");
    }

    let version = env::var("CARGO_PKG_VERSION").unwrap();
    let expected_tag = format!("v{version}");
    let exact_tag = git(
        &manifest_dir,
        &[
            "describe",
            "--tags",
            "--exact-match",
            "--match",
            &expected_tag,
        ],
    );
    let dirty = git(&manifest_dir, &["status", "--porcelain"])
        .map(|status| !status.is_empty())
        .unwrap_or(true);
    if exact_tag.as_deref() == Some(expected_tag.as_str()) && !dirty {
        return;
    }

    let source_ref = if dirty {
        "main".to_string()
    } else {
        git(&manifest_dir, &["rev-parse", "HEAD"]).unwrap_or_else(|| "main".to_string())
    };
    println!("cargo:rustc-env=ALEMBIC_SOURCE_REF={source_ref}");
}
