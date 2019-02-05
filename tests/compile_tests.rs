extern crate compiletest_rs as compiletest;

#[cfg(all(feature = "test-utils", feature = "compiletests"))]
use std::path::PathBuf;

#[cfg(all(feature = "test-utils", feature = "compiletests"))]
fn run_mode(mode: &'static str) {
    let mut config = compiletest::Config::default().tempdir();

    config.mode = mode.parse().expect("Invalid mode");
    config.src_base = PathBuf::from(format!("tests/{}", mode));
    config.target_rustcflags = Some("-L target/debug -L target/debug/deps".to_string());
    // config.link_deps();
    config.clean_rmeta();

    compiletest::run_tests(&config);
}

#[cfg(all(feature = "test-utils", feature = "compiletests"))]
#[test]
fn compile_test() {
    run_mode("compile-fail");
}
