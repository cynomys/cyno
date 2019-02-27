mod files;
mod genome;
mod dg;

use std::path::Path;
use std::collections::HashMap;

fn main() -> Result<(), std::io::Error> {
    let fs = files::get_fasta_path(Path::new("./data/"))?;
    println!("{:?}", fs);

    let all_kmers = genome::get_kmers_fastas(&fs, 11)?;

    // dgraph init
    let dg_client = dg::create_dgraph_connection("10.139.14.202:9080")?;
    dg::drop_all(&dg_client)?;

    let schema_payload = dg::set_schema(&dg_client);
    dg::add_genomes_dgraph(&dg_client, &all_kmers)?;


    println!("Schema Payload: {:?}", schema_payload);
    println!("Done");
    Ok(())
}
