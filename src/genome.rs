use bio::io::fasta;

use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;

#[derive(Debug)]
pub struct ContigKmers<'a>{
    name: String,
    kmers: Vec<&'a str>
}

pub fn get_kmers_fastas<'a>(fs: &Vec<PathBuf>)
    -> Result<HashMap<String, Vec<ContigKmers<'a>>>, Error> {
    let mut hm = HashMap::new();

    // This is the test data
    let s: &str = "ATCGGCGGCGT";

    let kmer1 = ContigKmers {
        name: String::from("genomeA"),
        kmers: vec![&s[0..3]]
    };

    hm.insert(String::from("genomeA"), vec![kmer1]);
    // End of the manual test data

    for ffile in fs{
        let reader = fasta::Reader::from_file(&ffile)?;
        for record in reader.records(){
            let r = record?;

        }
    }


    Ok(hm)
}
