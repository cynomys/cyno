use clap::{App, Arg, ArgMatches};
use std::io::{Error, ErrorKind};

pub fn get_args() -> Result<ArgMatches<'static>, Error> {
    let args = App::new("cyno")
        .version("0.1.0")
        .arg(
            Arg::with_name("input")
                .short("i")
                .required(true)
                .index(1)
                .help("The input file or directory of fasta-formatted sequences"),
        )
        .arg(
            Arg::with_name("chunk size")
                .help("The number of kmers to process at once")
                .short("c")
                .long("chunk_size")
                .default_value("100000")
        )
        .get_matches();

    match args.value_of("input"){
        Some(m) => {println!("Running with {}", m);
            Ok(args)
        }
        None=> Err(Error::new(ErrorKind::NotFound, "Input file not found"))
    }
}
