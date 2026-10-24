use anyhow::Result;
use vergen_gitcl::{Build, Cargo, Emitter, Gitcl};

fn main() -> Result<()> {
    let build = Build::builder().build_timestamp(true).build();
    let cargo = Cargo::builder()
        .target_triple(true)
        .debug(true)
        .opt_level(true)
        .features(true)
        .build();
    // Describe with lightweight tags and a `-dirty` suffix. Outside a git
    // checkout (e.g. a crates.io or git-dependency build) the git instructions
    // are skipped with a warning, so the `VERGEN_GIT_*` reads are `option_env!`.
    let git = Gitcl::builder()
        .describe(true, true, None)
        .sha(false)
        .commit_timestamp(true)
        .branch(true)
        .build();
    Emitter::default()
        .add_instructions(&build)?
        .add_instructions(&cargo)?
        .add_instructions(&git)?
        .emit()
}
