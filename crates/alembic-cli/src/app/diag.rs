pub(super) fn err(scope: &str, message: &str) {
    eprintln!("error[{scope}]: {message}");
}
