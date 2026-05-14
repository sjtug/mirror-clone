use vergen_gitcl::{Emitter, GitclBuilder};

fn main() {
    let gitcl = GitclBuilder::default()
        .sha(true)
        .build()
        .expect("failed to configure vergen git metadata");

    Emitter::default()
        .add_instructions(&gitcl)
        .expect("failed to collect vergen git metadata")
        .emit()
        .expect("failed to emit vergen git metadata");
}
