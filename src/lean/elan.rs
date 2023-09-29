use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct ElanConfig {
    #[structopt(long, default_value = "3")]
    pub retain_elan_versions: usize,
    #[structopt(long, default_value = "30")]
    pub retain_lean_versions: usize,
}
