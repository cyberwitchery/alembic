mod support;

use std::fs;
use std::process::Command;
use support::{
    bin_path, django_available, django_full_stack_available, fixture_path, python_path,
    run_apply_django, run_command, run_command_capture, walkthrough_path, write_django_config,
};
use tempfile::tempdir;

/// written into the temp dir and run with the configured interpreter.
const DRIVE_API: &str = include_str!("support/drive_django_api.py");
const DRIVE_DATETIME: &str = include_str!("support/drive_django_datetime.py");

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

// `validate` format-checks `date`, `datetime` and `time` as rfc 3339 so that a
// malformed timestamp is caught before apply. that rule is only worth anything if
// the shapes it *accepts* are ones a real column takes, and the walkthrough e2e
// above never exercises one: django-dcim.yaml declares no date field. this drives
// the edges of the accepted set through `manage.py loaddata`, which parses each
// value with django's own `DateField`/`DateTimeField`/`TimeField`.

#[test]
fn django_loads_every_date_shape_validate_accepts() {
    let python = python_path();
    if !django_available(&python) {
        eprintln!(
            "skipping django datetime e2e; django + djangorestframework not available for {python}"
        );
        return;
    }

    let out = tempdir().expect("temp dir");
    let app_out = out.path().join("app");
    let config = write_django_config(out.path(), &app_out);
    let plan = out.path().join("plan.json");
    let state = out.path().join("state.json");

    // `plan` loads the inventory, so it is also the validate gate: a shape the
    // rfc 3339 check rejected would fail here rather than at loaddata.
    let mut plan_cmd = Command::new(bin_path());
    plan_cmd.env("ALEMBIC_STATE_PATH", &state);
    plan_cmd.args([
        "plan",
        "--backend-config",
        config.to_str().unwrap(),
        "-f",
        fixture_path("django_datetime.yaml").to_str().unwrap(),
        "-o",
        plan.to_str().unwrap(),
    ]);
    run_command(plan_cmd, "plan django datetime fixture");

    // apply runs migrate and then loaddata; loaddata is where django parses the
    // values, so a value validate accepts and django refuses fails right here.
    let mut apply_cmd = Command::new(bin_path());
    apply_cmd.env("ALEMBIC_STATE_PATH", &state);
    apply_cmd.args([
        "apply",
        "--backend-config",
        config.to_str().unwrap(),
        "--plan",
        plan.to_str().unwrap(),
    ]);
    run_command(apply_cmd, "apply django datetime fixture");

    let models = fs::read_to_string(app_out.join("alembic_app").join("generated_models.py"))
        .expect("generated_models.py should exist");
    // the columns really are date columns; a CharField would load any string.
    assert!(models.contains("starts_on = models.DateField("), "{models}");
    assert!(
        models.contains("starts_at = models.DateTimeField("),
        "{models}"
    );
    assert!(models.contains("opens_at = models.TimeField("), "{models}");

    let script = out.path().join("drive_django_datetime.py");
    fs::write(&script, DRIVE_DATETIME).expect("write the datetime driver into the temp dir");
    let mut drive = Command::new(&python);
    drive.arg(&script).arg(&app_out);
    print!(
        "{}",
        run_command_capture(drive, "read back the django date columns")
    );
}

// with django-filter and drf-spectacular installed the generated app takes its
// full path. generating it is not enough: the routes have to answer, and a
// filtered list has to actually filter -- the failure this guards is a filter
// that silently returns every row.

#[test]
fn django_generated_api_filters_and_serves_its_schema() {
    let python = python_path();
    if !django_full_stack_available(&python) {
        eprintln!(
            "skipping django api e2e; django, djangorestframework, django-filter \
             and drf-spectacular are not all available for {python}"
        );
        return;
    }

    let out = tempdir().expect("temp dir");
    let app_out = out.path().join("app");
    let config = write_django_config(out.path(), &app_out);
    let plan = out.path().join("plan.json");
    let state = out.path().join("state.json");

    let mut plan_cmd = Command::new(bin_path());
    plan_cmd.env("ALEMBIC_STATE_PATH", &state);
    plan_cmd.args([
        "plan",
        "--backend-config",
        config.to_str().unwrap(),
        "-f",
        fixture_path("django_api.yaml").to_str().unwrap(),
        "-o",
        plan.to_str().unwrap(),
    ]);
    run_command(plan_cmd, "plan django api fixture");

    let mut apply_cmd = Command::new(bin_path());
    apply_cmd.env("ALEMBIC_STATE_PATH", &state);
    apply_cmd.args([
        "apply",
        "--backend-config",
        config.to_str().unwrap(),
        "--plan",
        plan.to_str().unwrap(),
    ]);
    run_command(apply_cmd, "apply django api fixture");

    // pinned here so a missing declaration localises before the api driver runs.
    let settings = fs::read_to_string(app_out.join("alembic_project").join("settings.py"))
        .expect("settings.py should exist");
    assert!(
        settings.contains("django_filters.rest_framework.DjangoFilterBackend"),
        "{settings}"
    );
    let views = fs::read_to_string(app_out.join("alembic_app").join("generated_views.py"))
        .expect("generated_views.py should exist");
    assert!(
        views.lines().any(|line| {
            line.trim_start().starts_with("filterset_fields = [") && line.contains("\"role\"")
        }),
        "{views}"
    );
    let urls = fs::read_to_string(app_out.join("alembic_app").join("generated_urls.py"))
        .expect("generated_urls.py should exist");
    assert!(urls.contains("router.register(\"dcimdevices\""), "{urls}");
    assert!(urls.contains("SpectacularAPIView.as_view()"), "{urls}");

    let script = out.path().join("drive_django_api.py");
    fs::write(&script, DRIVE_API).expect("write the api driver into the temp dir");
    let mut drive = Command::new(&python);
    drive.arg(&script).arg(&app_out);
    print!(
        "{}",
        run_command_capture(drive, "drive the generated django api")
    );
}
