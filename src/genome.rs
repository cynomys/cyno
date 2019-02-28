use crate::files;

use bio::io::fasta;

use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::str;
use std::str::from_utf8;


// Data for storing the contig name and sequence
// The get_kmers_contig() function is what is used when creating
// the kmers for insertion into the graph. The data structure just stores
// a copy of the String until it is needed for generating the kmers.
#[derive(Debug)]
pub struct ContigKmers{
    name: String,
    contig_seq: String,
    kmer_length: usize
}

impl ContigKmers{
    pub fn get_kmers_contig(&self) -> Vec<&str>{
        let mut the_kmers: Vec<& str> = Vec::new();
        let the_seq = self.contig_seq.as_str();

        for i in 0 .. (self.contig_seq.len() - self.kmer_length - 2){
            the_kmers.push(&the_seq[i .. i + self.kmer_length - 1]);
        }
       the_kmers
    }
}

// Create a HashMap to be returned, with every genome name being based on the
// file of the genome as they key, and a Vec<ContigKmers> as the value, which holds the
// sequence for every contig and the contig name. Method for generating kmers as needed
// for each contig is included.
pub fn get_kmers_fastas<'a>(fs: &Vec<PathBuf>, k_size: usize)
    -> Result<HashMap<String, Vec<ContigKmers>>, Error> {
    let mut hm = HashMap::new();

    for ffile in fs{
        let reader = fasta::Reader::from_file(&ffile)?;
        for record in reader.records(){
            let r = record.unwrap();
            let rseq = str::from_utf8(r.seq()).unwrap();

            let next_contig = ContigKmers{
                name: r.id().to_owned(),
                contig_seq: rseq.to_owned(),
                kmer_length: k_size
            };

            // Get genome name as Blake2 hash of file
            let genome_name = files::get_blake2_file(ffile)?;

            hm.insert(genome_name.to_owned(), vec![next_contig]);
        }
    }
    Ok(hm)
}
