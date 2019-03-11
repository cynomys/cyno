mod dg;
mod files;
mod cl;

use std::path::Path;
use std::io::{Error, ErrorKind};

fn main() -> Result<(), std::io::Error> {
    let args = cl::get_args()?;
    let chunk_size =match args.value_of("chunk_size").unwrap().parse::<usize>(){
        Ok(m) => m,
        Err(..) => return Err(Error::new(ErrorKind::Other, "Could not convert string to int from CLAP"))
    };

    let fs = files::get_fasta_path(Path::new(args.value_of("input").unwrap()))?;

    // dgraph init
    let dg_client = dg::create_dgraph_connection("10.139.14.193:9080")?;
    dg::drop_all(&dg_client)?;
    dg::set_schema(&dg_client)?;
    dg::add_genomes_dgraph(dg_client, &fs, 11, chunk_size)?;

    println!("Done");
    Ok(())
}
