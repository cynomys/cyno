mod dg;
mod files;
mod genome;

use std::path::Path;

fn main() -> Result<(), std::io::Error> {
    let fs = files::get_fasta_path(Path::new("./data/E_coli.fasta"))?;
    println!("{:?}", fs);

//    let parsed_genomes = genome::get_parsed_genomes(&fs, 11)?;

    // dgraph init
    let dg_client = dg::create_dgraph_connection("10.139.14.193:9080")?;
    dg::drop_all(&dg_client)?;

    dg::set_schema(&dg_client)?;

    dg::add_genomes_dgraph(dg_client, &fs, 6000000, 11)?;

    println!("Done");
    Ok(())
}
