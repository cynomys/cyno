mod dg;
mod files;
mod genome;

use std::path::Path;

fn main() -> Result<(), std::io::Error> {
    let fs = files::get_fasta_path(Path::new("./data/Salmonella.fasta"))?;
    println!("{:?}", fs);

    let parsed_genomes = genome::get_parsed_genomes(&fs, 11)?;

    // dgraph init
    let dg_client = dg::create_dgraph_connection("10.139.14.193:9080")?;
    dg::drop_all(&dg_client)?;

    dg::set_schema(&dg_client)?;

    let mut empty_quads: Vec<Vec<String>> = Vec::new();
    let final_quads = dg::add_genomes_dgraph(dg_client, parsed_genomes, 6000000, &mut empty_quads)?;

    files::write_final_quads(Path::new("./test_out.txt"), final_quads)?;

    println!("Done");
    Ok(())
}
