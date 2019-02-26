mod files;
mod genome;
mod dg;

use dgraph;

use std::path::Path;
use std::collections::HashMap;

fn main() -> Result<(), std::io::Error> {
    let fs = files::get_fasta_path(Path::new("./data/"))?;
    println!("{:?}", fs);

    let all_kmers = genome::get_kmers_fastas(&fs, 11)?;


    // dgraph init
    let dg = dg::create_dgraph_connection("10.139.14.202:9080");

    println!("Done");
    Ok(())
}
