use crate::files;
use bio::io::fasta;
use dgraph::{make_dgraph, Dgraph, Mutation, Operation, Payload};
use rayon::prelude::*;
use serde::Deserialize;
use serde_json;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};

// Data structures for dgraph
#[derive(Deserialize, Debug)]
struct Node {
    uid: String,
    kmer: String,
}

#[derive(Deserialize, Debug)]
struct FindAll {
    find_all: Vec<Node>,
}

#[derive(Debug)]
struct KmerLink {
    k1: String,
    k2: String,
    edge: String,
}

// New DB
pub fn create_dgraph_connection(addr: &str) -> Result<dgraph::Dgraph, Error> {
    let cx = make_dgraph!(dgraph::new_dgraph_client(addr));
    Ok(cx)
}

// Clean DB
pub fn drop_all(client: &Dgraph) -> Result<Payload, Error> {
    let op_drop = Operation {
        drop_all: true,
        ..Default::default()
    };

    let r = client.alter(&op_drop);
    match r {
        Ok(r) => Ok(r),
        Err(r) => Err(Error::new(
            ErrorKind::Other,
            format!("Could not drop all data from graph. {}", r),
        )),
    }
}

// Initial schema
pub fn set_schema(client: &Dgraph) -> Result<Payload, Error> {
    let op_schema = Operation {
        schema: "kmer: string @index(exact, term) .".to_string(),
        ..Default::default()
    };

    let r = client.alter(&op_schema);
    match r {
        Ok(r) => Ok(r),
        Err(..) => Err(Error::new(ErrorKind::Other, "Could not set dgraph schema")),
    }
}

// Store on a per-genome basis
pub fn add_genomes_dgraph(
    client: Dgraph,
    files: &Vec<PathBuf>,
    kmer_size: usize,
) -> Result<(), Error> {
    // Iterate through all genomes
    // We keep a HashMap of all known kmer: uid to avoid duplications
    // and speed up construction of the quads
    let kmer_uid: HashMap<String, String> = HashMap::new();
    let arc_kmer_uid = Arc::new(Mutex::new(kmer_uid));
    // One genome at a time
    for file in files {
        // Get genome name as Blake2 hash of file and add a leading 'g' as predicates cannot start
        // as numbers.
        let genome_name = format!("g{}", files::get_blake2_file(file)?);
        println!("Adding genome {:?}", genome_name);
        add_genome_schema(&client, &genome_name)?;

        let reader = fasta::Reader::from_file(&file)?;

        // Each record is a contig, but we need to collect them into a Vec so that
        // we can parallel process them using rayon
        let rx = reader.records().collect::<Vec<_>>();
        let all_kmer_links = rx
            .par_iter()
            .flat_map(|record| {
                // We need the .as_ref() otherwise the compiler thinks we are borrowing record
                // and we will not be able to continue
                let r = record.as_ref().unwrap();
                println!("{:?}", r.id());

                // Turn contig into a window of kmers
                let kmers = r
                    .seq()
                    .windows(kmer_size)
                    .map(|x| from_utf8(x).unwrap())
                    .collect::<Vec<&str>>();

                let arc_kmer_uid = arc_kmer_uid.clone();

                // Update the one-true HashMap
                let new_hm = query_batch_dgraph(&client, &kmers).unwrap();
                let mut kmer_uid = arc_kmer_uid.lock().unwrap();
                for (k, v) in new_hm {
                    kmer_uid.insert(k, v);
                }

                // Return the created KmerLinks for future processing
                let klinks = create_kmer_links(&kmers, &kmer_uid, &genome_name);
                klinks
            })
            .collect::<Vec<KmerLink>>(); // end contig

        // parallel insertion
        // dgraph live load uses batches of 1000, so we can mimic that here
        // each KmerLink has 3 quads, so ~333 to mimic dgraph live
        all_kmer_links
            .into_par_iter()
            .chunks(333)
            .for_each(|kmer_chunk| {
                add_batch_dgraph(&client, &kmer_chunk).unwrap();
                println!(".");
            });
    } // end file
    Ok(())
}

// Create all the quads we need
fn create_kmer_links<'a>(
    kmers: &Vec<&str>,
    hm: &HashMap<String, String>,
    genome_name: &str,
) -> Vec<KmerLink> {
    let mut new_quads = Vec::new();

    for i in 0..kmers.len() - 2 {
        // Grab the existing uid or create a new one for each kmer
        let k1_uid = match hm.get(kmers[i]) {
            Some(m) => m.to_owned(),
            None => create_uid_kmer(kmers[i]),
        };

        let k2_uid = match hm.get(kmers[i + 1]) {
            Some(m) => m.to_owned(),
            None => create_uid_kmer(kmers[i + 1]),
        };

        let mut k1_node = String::with_capacity(
            1 + k1_uid.len() + " <kmer> ".len() + kmers[i].len() + "\"\" .".len(),
        );
        k1_node.push_str(&k1_uid);
        k1_node.push_str(" <kmer> \"");
        k1_node.push_str(kmers[i]);
        k1_node.push_str("\" .");

        let mut k2_node = String::with_capacity(
            1 + k2_uid.len() + " <kmer> ".len() + kmers[i + 1].len() + "\"\" .".len(),
        );
        k2_node.push_str(&k2_uid);
        k2_node.push_str(" <kmer> \"");
        k2_node.push_str(kmers[i + 1]);
        k2_node.push_str("\" .");

        let mut edge = String::with_capacity(
            1 + k1_uid.len() + genome_name.len() + " <> ".len() + k2_uid.len() + " .".len(),
        );
        edge.push_str(&k1_uid);
        edge.push_str(" <");
        edge.push_str(genome_name);
        edge.push_str("> ");
        edge.push_str(&k2_uid);
        edge.push_str(" .");

        // Include space for the newlines
        let new_link = KmerLink {
            k1: k1_node,
            k2: k2_node,
            edge: edge,
        };

        new_quads.push(new_link);
    }
    new_quads
}

// Use the HashMap as the db for an "upsert" of the uid
fn _upsert_uid(hm: &mut HashMap<String, String>, k: &str) -> String {
    // Check to see if kmer is already in the graph
    // If it is, grab the uid, if not, use a blank node
    match hm.get(k) {
        Some(v) => v.to_owned(),
        None => create_uid_kmer(k),
    }
}

// Destructure into a string for addition to dgraph
fn get_string_kmerlink(kx: &KmerLink) -> String {
    let mut triple_string = String::with_capacity(4 + kx.k1.len() + kx.k2.len() + kx.edge.len());
    triple_string.push_str(&kx.k1);
    triple_string.push('\n');
    triple_string.push_str(&kx.k2);
    triple_string.push('\n');
    triple_string.push_str(&kx.edge);
    triple_string.push('\n');

    triple_string
}

fn create_uid_kmer(k: &str) -> String {
    // If we pre-allocate the string-size, building it is much more efficient
    // Use the k prefix to denote the blank node
    // In cases where the same kmer exists more than once in the batch, we don't
    // wan't to assign a new node to it, we want to re-use the blank node.
    // Therefore we will insert the blank node into the HashMap.
    // In any future queries of the graph, the actual uid will be returned
    // and wipe out the blank node.
    let mut uid = String::with_capacity(4 + k.len());
    uid.push_str("_:k");
    uid.push_str(k);
    uid
}

// Batch add the kmers,
fn add_batch_dgraph(client: &Dgraph, kmer_links: &Vec<KmerLink>) -> Result<(), Error> {
    let mut txn = client.new_txn();
    let mut mutation = Mutation::new();
    // Manual error propagation for now
    // The data is expected to be in u8 form for submission
    // Create a single String from the Vec<String> and convert to bytes
    let mut sx = String::new();
    for kl in kmer_links {
        sx.push_str(&get_string_kmerlink(kl));
    }

    mutation.set_set_nquads(sx.as_bytes().to_owned());
    //    println!("{:?}", mutation);

    let m = txn.mutate(mutation);
    match m {
        Ok(m) => m,
        Err(m) => {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Failed to insert NQuads. {}", m),
            ));
        }
    };

    // Commit
    let cc = txn.commit();
    match cc {
        Ok(..) => Ok(()),
        Err(..) => return Err(Error::new(ErrorKind::Other, "Transaction failed")),
    }
}

// Query our group of strains, updating the one true HashMap
fn query_batch_dgraph(
    client: &Dgraph,
    kmers: &Vec<&str>,
) -> Result<HashMap<String, String>, Error> {
    let query = r#"query find_all($klist: string){
            find_all(func: anyofterms(kmer, $klist))
            {
                uid
                kmer
            }
    }"#
    .to_string();

    let mut variables = HashMap::new();
    variables.insert("$klist".to_string(), kmers.join(" "));

    let resp = client.new_readonly_txn().query_with_vars(query, variables);
    let r = match resp {
        Ok(resp) => resp,
        Err(resp) => {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Query failed {}", resp),
            ));
        }
    };
    let r_json: FindAll = serde_json::from_slice(&r.json)?;

    // Update the HashMap for existing values
    // find_all contains a Vec<Node>
    let mut new_hm = HashMap::new();
    for q in r_json.find_all {
        // uids need to be wrapped in <> for use in NQuad format
        // We will add them here, so that any return from the one true HashMap
        // can be used directly as the uid
        let mut uid = String::with_capacity(1 + "<>".len() + q.uid.len());
        uid.push('<');
        uid.push_str(&q.uid);
        uid.push('>');

        new_hm.insert(q.kmer, uid.to_owned());
    }

    Ok(new_hm)
}

fn add_genome_schema(client: &Dgraph, genome: &str) -> Result<(), Error> {
    let op_schema = Operation {
        schema: format!("{}: uid .", genome),
        ..Default::default()
    };

    let r = client.alter(&op_schema);
    match r {
        Ok(..) => Ok(()),
        Err(e) => Err(Error::new(
            ErrorKind::Other,
            format!("Could not add genome {} to dgraph schema. {}", genome, e),
        )),
    }
}
