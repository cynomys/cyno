use std::io::{Error, ErrorKind};
use bio::io::fasta;
use std::path::{Path, PathBuf};
use std::fs::{self};

/// Returns a vector of all the fasta files in a given path (file or directory)
///
/// # Examples
///
/// # /dir/fasta1.fasta
/// # /dir/fasta2.fasta
/// # /dir/not_fasta.txt
///
/// ```
///
/// let fasta_files = get_fasta_path("/dir/")
/// assert_eq!(vec!["/dir/fasta1.fasta", "/dir/fasta2.fasta"], fasta_files)
///
/// let fasta_file = get_fasta_path("/path/to/my.fasta")
/// assert_eq!(vec!["/path/to/my.fasta"], fasta_file)
/// ```
pub fn get_fasta_path(file_or_dir: &Path) -> Result<Vec<PathBuf>, Error> {
    if file_or_dir.is_file(){
        Ok(vec![file_or_dir.to_path_buf()])
    }
    else if file_or_dir.is_dir(){
        let all_files= recurse_directory(file_or_dir)?;
        Ok(all_files)
    }
    else{
        Err(Error::new(ErrorKind::NotFound, "missing file"))
    }
}


// Path only holds a reference to the path string
// PathBuf owns the string
fn recurse_directory(p: &Path) -> Result<Vec<PathBuf>, Error>{
    let mut af: Vec<PathBuf> = Vec::new();
    for entry in fs::read_dir(p)?{
        let e = entry?;
        let path = e.path();

        if path.is_dir(){
            recurse_directory(&path)?;
        }
        else{
            af.push(path);
        }
    }
    Ok(af)
}

pub fn get_kmers_file() {
    let reader = fasta::Reader::from_file("./data/Salmonella.fasta").unwrap();

    for record in reader.records() {
        let r = record.unwrap();
        println!("{:?}", String::from_utf8_lossy(r.seq()));
    }
}
