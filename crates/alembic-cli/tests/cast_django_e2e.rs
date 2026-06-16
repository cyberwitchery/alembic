mod support;

use support::run_apply_django;

#[test]
fn cast_django_e2e_minimal() {
    run_apply_django("minimal_plan.json");
}

#[test]
fn cast_django_e2e_relations() {
    run_apply_django("relations_plan.json");
}
