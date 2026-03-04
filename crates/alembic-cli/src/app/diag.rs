pub(super) fn warn(scope: &str, message: &str) {
    eprintln!("warning[{scope}]: {message}");
}

pub(super) fn err(scope: &str, message: &str) {
    eprintln!("error[{scope}]: {message}");
}
