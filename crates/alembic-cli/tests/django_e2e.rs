mod support;

use std::fs;
use std::process::Command;
use support::{
    bin_path, django_available, python_path, run_apply_django, run_command, walkthrough_path,
    write_django_config,
};
use tempfile::tempdir;

#[test]
fn django_e2e_minimal() {
    run_apply_django("minimal_plan.json");
}

#[test]
fn django_e2e_relations() {
    run_apply_django("relations_plan.json");
}

// the documented django flow (docs/examples/03-django-dcim.md): `plan` turns the
// walkthrough inventory into an all-creates plan, then `apply` generates the app.

#[test]
fn django_plan_from_documented_walkthrough_is_all_creates() {
    // the plan step needs no django: a write-only backend is planned as all-creates.
    let out = tempdir().expect("temp dir");
    let config = write_django_config(out.path(), &out.path().join("app"));
    let plan = out.path().join("plan.json");

    let mut cmd = Command::new(bin_path());
    cmd.env("ALEMBIC_STATE_PATH", out.path().join("state.json"));
    cmd.args([
        "plan",
        "--backend-config",
        config.to_str().unwrap(),
        "-f",
        walkthrough_path("django-dcim.yaml").to_str().unwrap(),
        "-o",
        plan.to_str().unwrap(),
    ]);
    run_command(cmd, "plan django walkthrough");

    let plan_json = fs::read_to_string(&plan).expect("plan file written");
    // both walkthrough objects are planned, and only as creates (Op is `{"op": ...}`).
    assert!(plan_json.contains("dcim.site"), "{plan_json}");
    assert!(plan_json.contains("dcim.device"), "{plan_json}");
    assert!(plan_json.contains("\"op\": \"create\""), "{plan_json}");
    assert!(!plan_json.contains("\"op\": \"update\""), "{plan_json}");
    assert!(!plan_json.contains("\"op\": \"delete\""), "{plan_json}");
}

#[test]
fn django_documented_walkthrough_flow_generates_app() {
    let python = python_path();
    if !django_available(&python) {
        eprintln!("skipping django e2e; django + djangorestframework not available for {python}");
        return;
    }

    let out = tempdir().expect("temp dir");
    let app_out = out.path().join("app");
    let config = write_django_config(out.path(), &app_out);
    let plan = out.path().join("plan.json");
    let state = out.path().join("state.json");

    // step 1: plan the walkthrough inventory into an all-creates plan.
    let mut plan_cmd = Command::new(bin_path());
    plan_cmd.env("ALEMBIC_STATE_PATH", &state);
    plan_cmd.args([
        "plan",
        "--backend-config",
        config.to_str().unwrap(),
        "-f",
        walkthrough_path("django-dcim.yaml").to_str().unwrap(),
        "-o",
        plan.to_str().unwrap(),
    ]);
    run_command(plan_cmd, "plan django (documented flow)");

    // step 2: apply the plan to generate the app.
    let mut apply_cmd = Command::new(bin_path());
    apply_cmd.env("ALEMBIC_STATE_PATH", &state);
    apply_cmd.args([
        "apply",
        "--backend-config",
        config.to_str().unwrap(),
        "--plan",
        plan.to_str().unwrap(),
    ]);
    run_command(apply_cmd, "apply django (documented flow)");

    // the generated models mirror the walkthrough: a site and a device with a site FK.
    let models = fs::read_to_string(app_out.join("alembic_app").join("generated_models.py"))
        .expect("generated_models.py should exist");
    assert!(models.contains("class DcimSite"), "{models}");
    assert!(models.contains("class DcimDevice"), "{models}");
    assert!(
        models.contains("models.ForeignKey(\"DcimSite\""),
        "{models}"
    );
}
