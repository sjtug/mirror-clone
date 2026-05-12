use vergen::EmitBuilder;

fn main() {
    // Use vergen for build metadata (per developer suggestion)
    EmitBuilder::builder()
        .build_timestamp()
        .emit()
        .unwrap_or_else(|e| eprintln!("vergen warning: {e}"));

    // Always get fresh git SHA via git command (avoids vergen idempotent caching)
    // Note: need safe.directory for Docker builds where uid differs
    let _ = std::process::Command::new("git")
        .args(["config", "--global", "--add", "safe.directory", "*"])
        .output();

    if let Ok(output) = std::process::Command::new("git")
        .args(["rev-parse", "--short=10", "HEAD"])
        .output()
    {
        let hash = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !hash.is_empty() {
            println!("cargo:rustc-env=VERGEN_GIT_SHA={hash}");
            println!("cargo:rerun-if-changed=.git/HEAD");
        } else {
            println!("cargo:rustc-env=VERGEN_GIT_SHA=unknown");
        }
    } else {
        println!("cargo:rustc-env=VERGEN_GIT_SHA=unknown");
    }
}
