use bio::io::fasta;

use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::str;
use std::str::from_utf8;

#[derive(Debug)]
pub struct ContigKmers{
    name: String,
    contig_seq: String,
    kmer_length: usize
}

impl ContigKmers{
    fn get_kmers_contig(&self) -> Vec<&str>{
        let mut the_kmers: Vec<&str> = Vec::new();
        let the_seq = self.contig_seq.as_str();

        for i in 0 .. (self.contig_seq.len() - self.kmer_length - 1){
            the_kmers.push(&the_seq[i .. i + self.kmer_length - 1]);
        }
       the_kmers
    }
}

pub fn get_kmers_fastas<'a>(fs: &Vec<PathBuf>)
    -> Result<HashMap<String, Vec<ContigKmers>>, Error> {
    let mut hm = HashMap::new();

    // This is the test data
    // This works because the array has a fixed size that is known before compile time
    // When we are getting data from a file, we cannot know the size before compile time
    // and so have to take a different approach for the real data
    let bs = b"AAATTTCCTTTT";
    let bs_str = str::from_utf8(bs).unwrap();

    let kmer1 = ContigKmers {
        name: String::from("genomeA"),
        contig_seq: bs_str.to_owned(),
        kmer_length: 11
    };

    hm.insert(String::from("genomeA"), vec![kmer1]);
    // End of the manual test data


    for ffile in fs{
        let reader = fasta::Reader::from_file(&ffile)?;
        for record in reader.records(){
            let r = record.unwrap();
            let rr = r.seq();
            let rseq = str::from_utf8(rr).unwrap();

            let next_contig = ContigKmers{
                name: r.id().to_owned(),
                contig_seq: rseq.to_owned(),
                kmer_length: 11
            };

            hm.insert(ffile.to_str().unwrap().to_owned(), vec![next_contig]);
        }
    }
    Ok(hm)
}
