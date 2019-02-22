mod files;
use std::path::Path;

fn main() -> Result<(), std::io::Error> {
    let f = files::get_fasta_path(Path::new("./data/"))?;
    println!("{:?}", f);
    println!("Done!");
    Ok(())
}
