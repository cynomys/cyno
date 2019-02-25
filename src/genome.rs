use bio::io::fasta;

use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::str;

#[derive(Debug)]
pub struct ContigKmers<'a>{
    name: String,
    kmers: Vec<&'a str>
}

pub fn get_kmers_fastas<'a>(fs: &Vec<PathBuf>)
    -> Result<HashMap<String, Vec<ContigKmers<'a>>>, Error> {
    let mut hm = HashMap::new();

    // This is the test data
    let bs = b"AAATTTCCTTTT";
    let bs_str = str::from_utf8(bs).unwrap();

    let kmer1 = ContigKmers {
        name: String::from("genomeA"),
//        kmers: vec![]
        kmers: vec![&bs_str[0..3], &bs_str[4..6]]
    };

    hm.insert(String::from("genomeA"), vec![kmer1]);
    // End of the manual test data

    for ffile in fs{
        let reader = fasta::Reader::from_file(&ffile)?;
        for record in reader.records(){
            let r = record?;

//            let next_contig = ContigKmers{
//                name: r.id().to_owned()
//                // TODO: The bio module has DNA as u8, not str
//                // kmers: vec![&r.seq().to_vec()[0..3]]
//            };
//
//            hm.insert(ffile.to_string(), vec![next_contig])
        }
    }


    Ok(hm)
}
