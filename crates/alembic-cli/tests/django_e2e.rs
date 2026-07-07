mod support;

use support::run_apply_django;

#[test]
fn django_e2e_minimal() {
    run_apply_django("minimal_plan.json");
}

#[test]
fn django_e2e_relations() {
    run_apply_django("relations_plan.json");
}
