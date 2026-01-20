mod support;

use support::run_cast;

#[test]
fn cast_django_e2e_minimal() {
    run_cast("minimal.yaml");
}

#[test]
fn cast_django_e2e_relations() {
    run_cast("relations.yaml");
}
