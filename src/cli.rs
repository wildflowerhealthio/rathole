use clap::{ArgGroup, Parser};
use lazy_static::lazy_static;

#[derive(clap::ValueEnum, Clone, Debug, Copy)]
pub enum KeypairType {
    X25519,
    X448,
}

lazy_static! {
    static ref VERSION: &'static str =
        option_env!("VERGEN_GIT_DESCRIBE").unwrap_or(env!("CARGO_PKG_VERSION"));
    static ref LONG_VERSION: String = format!(
        "
Build Timestamp:     {}
Build Version:       {}
Commit SHA:          {:?}
Commit Date:         {:?}
Commit Branch:       {:?}
cargo Target Triple: {}
cargo Debug:         {}
cargo Opt Level:     {}
cargo Features:      {}
",
        env!("VERGEN_BUILD_TIMESTAMP"),
        env!("CARGO_PKG_VERSION"),
        option_env!("VERGEN_GIT_SHA"),
        option_env!("VERGEN_GIT_COMMIT_TIMESTAMP"),
        option_env!("VERGEN_GIT_BRANCH"),
        env!("VERGEN_CARGO_TARGET_TRIPLE"),
        env!("VERGEN_CARGO_DEBUG"),
        env!("VERGEN_CARGO_OPT_LEVEL"),
        env!("VERGEN_CARGO_FEATURES")
    );
}

#[derive(Parser, Debug, Default, Clone)]
#[command(
    about,
    version = *VERSION,
    long_version = LONG_VERSION.as_str(),
)]
#[command(group(
            ArgGroup::new("cmds")
                .required(true)
                .args(["CONFIG", "genkey"]),
        ))]
pub struct Cli {
    /// The path to the configuration file
    ///
    /// Running as a client or a server is automatically determined
    /// according to the configuration file.
    #[arg(id = "CONFIG")]
    pub config_path: Option<std::path::PathBuf>,

    /// Run as a server
    #[arg(long, short, group = "mode")]
    pub server: bool,

    /// Run as a client
    #[arg(long, short, group = "mode")]
    pub client: bool,

    /// Generate a keypair for the use of the noise protocol
    ///
    /// The DH function to use is x25519
    #[arg(long, value_enum, value_name = "CURVE")]
    pub genkey: Option<Option<KeypairType>>,
}
