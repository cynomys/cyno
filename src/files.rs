use bio::io::fasta;
use blake2::{Blake2b, Digest};
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::{fs, io};
use std::io::Write;

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
    if file_or_dir.is_file() {
        Ok(vec![file_or_dir.to_path_buf()])
    } else if file_or_dir.is_dir() {
        let all_files = recurse_directory(file_or_dir)?;

        // Of all the files, we want to keep only the fasta files
        let mut fasta_files: Vec<PathBuf> = Vec::new();

        // To ensure we only get fasta files, we open each file, and attempt
        // to get the first fasta record of each. If we succeed, we add the file to
        // the vector of fasta files. If not, we do nothing.
        for f in all_files {
            let reader = fasta::Reader::from_file(&f);
            match reader {
                Ok(gf) => {
                    for record in gf.records() {
                        match record {
                            Ok(..) => fasta_files.push(f),
                            Err(..) => {}
                        }
                        break;
                    }
                }
                Err(..) => {}
            }
        }

        // Check to see if any fasta files were found. If not, return an error
        if fasta_files.is_empty() {
            Err(Error::new(
                ErrorKind::NotFound,
                "No valid fasta files found",
            ))
        } else {
            Ok(fasta_files)
        }
    } else {
        Err(Error::new(ErrorKind::NotFound, "No valid files found"))
    }
}

// Path only holds a reference to the path string
// PathBuf owns the string
fn recurse_directory(p: &Path) -> Result<Vec<PathBuf>, Error> {
    let mut af: Vec<PathBuf> = Vec::new();
    for entry in fs::read_dir(p)? {
        let e = entry?;
        let path = e.path();

        if path.is_dir() {
            recurse_directory(&path)?;
        } else {
            af.push(path);
        }
    }
    Ok(af)
}

// Compute the hash of the file contents
pub fn get_blake2_file(f: &PathBuf) -> Result<String, Error> {
    let mut file_contents = fs::File::open(f)?;
    let mut hasher = Blake2b::new();

    //cargo cult from the docs
    io::copy(&mut file_contents, &mut hasher)?;
    let hash = hasher.result();

    // Convert the bytes into a string of lowercase hex with the :x trait
    Ok(format!("{:x}", hash))
}
