mod files;
use std::path::Path;

fn main() {
    let f = files::get_fasta_path(Path::new("./data/"));
    println!("{:?}", f);
    println!("Done!");
}
