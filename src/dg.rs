use crate::files;
use dgraph::{make_dgraph, Dgraph, Mutation, Operation, Payload};
use serde::Deserialize;
use serde_json;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::str::from_utf8;
//use std::sync::{Arc, Mutex};
use bio::io::fasta;

use std::path::{PathBuf};


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
    kmer_size: usize
) -> Result<(), Error> {
    // Iterate through all genomes
    // We keep a HashMap of all known kmer: uid to avoid duplications
    // and speed up construction of the quads
    let mut kmer_uid: HashMap<String, String> = HashMap::new();
    let mut quads: Vec<String> = Vec::new();

//    let arc_kmer_uid = Arc::new(Mutex::new(kmer_uid));
//    let arc_final_quads = Arc::new(Mutex::new(quads));

    // Client is read-only
//    let arc_client = Arc::new(client);

    // One genome at a time
    for file in files {
        // Get genome name as Blake2 hash of file
        let genome_name = files::get_blake2_file(file)?;
        let reader = fasta::Reader::from_file(&file)?;

        // Each record is a contig
        for record in reader.records() {
            let r = record.unwrap();
            // Turn contig into a window of kmers
            let kmer_window = r.seq().windows(kmer_size);

            // Add each kmer to the vec
            let mut dkmers = Vec::new();
            for k in kmer_window{
                let kmer = from_utf8(k).unwrap();
                dkmers.push(kmer);
            }

            query_batch_dgraph(&client, &mut kmer_uid, &dkmers).unwrap();
            quads.push(create_batch_quads(
                &dkmers,
                &mut kmer_uid,
                &genome_name,
            ));
        }
        add_batch_dgraph(&client, &quads)?;
    }
    Ok(())
}

// Create all the quads we need
fn create_batch_quads<'a>(
    kmers: &Vec<&str>,
    hm: &mut HashMap<String, String>,
    genome_name: &str,
) -> String {

    let mut new_quads = String::new();

    for i in 0..kmers.len() - 2 {
        let k1_uid = upsert_uid(hm, kmers[i]);
        let k2_uid = upsert_uid(hm, kmers[i + 1]);

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
        new_quads.push_str(&k1_node);
        new_quads.push('\n');
        new_quads.push_str(&k2_node);
        new_quads.push('\n');
        new_quads.push_str(&edge);
        new_quads.push('\n');
    }

    new_quads
}

// Use the HashMap as the db for an "upsert" of the uid
fn upsert_uid(hm: &mut HashMap<String, String>, k: &str) -> String {
    // Check to see if kmer is already in the graph
    // If it is, grab the uid, if not, use a blank node
    match hm.get(k) {
        Some(v) => v.to_owned(),
        None => {
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

            hm.insert(k.to_owned(), uid.to_owned());
            uid.to_owned()
        }
    }
}

// Batch add the kmers,
fn add_batch_dgraph(client: &Dgraph, nq: &Vec<String>) -> Result<(), Error> {
    let mut txn = client.new_txn();
    let mut mutation = Mutation::new();
    // Manual error propagation for now
    // The data is expected to be in u8 form for submission
    // Create a single String from the Vec<String> and convert to bytes
    mutation.set_set_nquads(nq.join("").as_bytes().to_owned());
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
    hmc: &mut HashMap<String, String>,
    kmers: &Vec<&str>,
) -> Result<(), Error> {
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
    for q in r_json.find_all {
        // uids need to be wrapped in <> for use in NQuad format
        // We will add them here, so that any return from the one true HashMap
        // can be used directly as the uid
        let mut uid = String::with_capacity(1 + "<>".len() + q.uid.len());
        uid.push('<');
        uid.push_str(&q.uid);
        uid.push('>');

        hmc.insert(q.kmer, uid.to_owned());
    }

    Ok(())
}
