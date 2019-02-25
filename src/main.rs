mod files;
mod genome;

use std::path::Path;
use std::collections::HashMap;

fn main() -> Result<(), std::io::Error> {
    let fs = files::get_fasta_path(Path::new("./data/"))?;
    println!("{:?}", fs);

    let all_kmers = genome::get_kmers_fastas(&fs)?;


    println!("Done");
    Ok(())
}
