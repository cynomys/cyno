use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "cyno",
    about = "Commandline loading of whole-genome kmer data into dgraph"
)]
#[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
pub struct Opt {
    #[structopt(parse(from_os_str))]
    pub input: PathBuf,

    #[structopt(short = "c", long = "chunk", default_value = "100000")]
    pub chunk: usize,

    #[structopt(short = "u", long = "url", default_value = "localhost:9080")]
    pub url: String,
}
