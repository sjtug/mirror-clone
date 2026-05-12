use std::process::Command;

fn main() {
    let git_hash = Command::new("git")
        .args(["rev-parse", "--short=10", "HEAD"])
        .output()
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=GIT_HASH={}", git_hash);
}
