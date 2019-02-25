mod files;
mod genome;

use std::path::Path;
use std::collections::HashMap;

fn main() -> Result<(), std::io::Error> {
    let fs = files::get_fasta_path(Path::new("./data/"))?;
    println!("{:?}", fs);

//    let mut all_kmers: HashMap<String, Vec<genome::ContigKmers>> = HashMap::new();
    let all_kmers = genome::get_kmers_fastas(&fs)?;

    for (k, v) in &all_kmers{
        println!("{:#?}: {:#?}", k, v);
        let kx = v[0].get_kmers_contig();
        println!("{:#?}", kx);
    }

    println!("Done");
    Ok(())
}
