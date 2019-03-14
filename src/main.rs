mod cl;
mod dg;
mod files;

use structopt::StructOpt;

fn main() -> Result<(), std::io::Error> {
    let args = cl::Opt::from_args();
    println!("{:?}", args.input);

    let fs = files::get_fasta_path(&args.input)?;

    // dgraph init
    let dg_client = dg::create_dgraph_connection(&args.url)?;
    dg::drop_all(&dg_client)?;
    dg::set_schema(&dg_client)?;

    // Iterate through all genomes and add to dgraph
    dg::add_genomes_dgraph(dg_client, &fs, 11)?;

    println!("Done");
    Ok(())
}
