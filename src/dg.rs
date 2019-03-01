use crate::genome;

use dgraph::{make_dgraph, Dgraph, Mutation, Operation, Payload};
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::str::from_utf8;
use serde::{Serialize, Deserialize};
use serde_json::{Value};

// Data structures for dgraph
#[derive(Deserialize, Debug)]
struct Node{
    uid: String,
    kmer: String
}

#[derive(Deserialize, Debug)]
struct FindAll{
    find_all: Vec<Node>
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
    client: &Dgraph,
    hm: &HashMap<String, Vec<genome::ContigKmers>>,
    chunk_size: usize,
) -> Result<(), Error> {
    // Iterate through all genomes
    // We keep a HashMap of all known kmer: uid to avoid duplications
    // and speed up construction of the quads
    let mut kmer_uid: HashMap<String, String> = HashMap::new();

    for (k, v) in hm {
        // Iterate through all contigs
        for contig in v {
            // Iterate through all kmers in the contig
            // The method returns a Window iterator of the kmer size
            // The windows are u8, so need to be converted into string
            let all_kmers = contig.get_kmers_contig();

            // We now want to collect chunks of the windowed kmers in chunk_size
            // For example, if chunk_size is 1000, this will give us a Vec of 1000
            // kmers as &[u8] that need to be converted into &str
            for kmer_chunks in all_kmers.collect::<Vec<_>>().chunks(chunk_size) {
                // Run the from_utf8.unwrap() function on every element of the kmer_chunks Vec
                // Create a Vec(&str) for use in querying and adding to dgraph
                let mut dkmers = Vec::new();
                for kmer in kmer_chunks{
                    let dk = from_utf8(kmer);
                    match dk{
                        Ok(dk) => dkmers.push(dk),
                        Err(dk) => return Err(Error::new(ErrorKind::Other, format!("Could not convert utf8 to string {}", dk)))
                    }
                }

                // Updates the kmer_uid HashMap with returned query values
                query_batch_dgraph(client, &kmer_uid, &dkmers)?;

                // Add new kmers as nodes and edges between them to the graph
                // Requires a string of newline separated quads
                let new_quads = create_batch_quads(&dkmers,&kmer_uid, &k);
//                add_batch_dgraph(client, )


            }

        }
    }

    Ok(())
}


// Create all the quads we need
fn create_batch_quads<'a>(kmers: &Vec<&str>, hm: &HashMap<String, String>, genome_name: &str) -> Vec<String>{
    let mut new_quads = Vec::new();

    for i in 0..kmers.len() - 2{
        let k1_uid = upsert_uid(hm, kmers[i]);
        let k2_uid = upsert_uid(hm, kmers[i+1]);

        let mut k1_node = String::with_capacity(1 + k1_uid.len() + " <kmer> ".len() + kmers[i].len() + " .".len());
        k1_node.push_str(&k1_uid);
        k1_node.push_str(" <kmer> ");
        k1_node.push_str(kmers[i]);
        k1_node.push_str(" .");
        new_quads.push(k1_node.to_owned());

        let mut k2_node = String::with_capacity(1 + k2_uid.len() + " <kmer> ".len() + kmers[i+1].len() + " .".len());
        k2_node.push_str(&k2_uid);
        k2_node.push_str(" <kmer> ");
        k2_node.push_str(kmers[i+1]);
        k2_node.push_str(" .");
        new_quads.push(k2_node.to_owned());

        let mut edge = String::with_capacity(1 + k1_uid.len() + genome_name.len() + "<>".len() + k2_uid.len() + " .".len());
        edge.push_str(&k1_uid);
        edge.push_str("<");
        edge.push_str(genome_name);
        edge.push_str(">");
        edge.push_str(&k2_uid);
        edge.push_str(" .");
        new_quads.push(edge.to_owned());
    };

    new_quads
}


// Use the HashMap as the db for an "upsert" of the uid
fn upsert_uid(hm: & HashMap<String, String>, k: &str) -> String{
    // Check to see if kmer is already in the graph
    // If it is, grab the uid, if not, use a blank node
    match hm.get(k){
        Some(k) => k.to_owned(),
        None => {
            // If we pre-allocate the string-size, building it is much more efficient
            let mut uid = String::with_capacity(4 + k.len());
            uid.push_str("_:k");
            uid.push_str(k);
            uid
        }
    }
}


// Batch add the kmers,
fn add_batch_dgraph(client: &Dgraph, nq: &str) -> Result<(), Error> {
    println!(".");
    // Insert one batch at a time
    let mut txn = client.new_txn();
    let mut mutation = Mutation::new();
    // Manual error propagation for now
    // The data is expected to be in u8 form for submission
    mutation.set_set_nquads(nq.as_bytes().to_owned());

    let m = txn.mutate(mutation);
    match m {
        Ok(m) => m,
        Err(..) => return Err(Error::new(ErrorKind::Other, "Failed to insert NQuads")),
    };

    // Commit
    let cc = txn.commit();
    match cc {
        Ok(..) => Ok(()),
        Err(..) => return Err(Error::new(ErrorKind::Other, "Transaction failed")),
    }
}


// Query our group of strains, updating the one true HashMap
fn query_batch_dgraph(client: &Dgraph, hmc: &HashMap<String, String>, kmers: &Vec<&str>) ->Result<(), Error>{
    let query = r#"query find_all($klist: string){
            find_all(func: anyofterms(kmer, $klist))
            {
                uid
                kmer
            }
    }"#.to_string();

    let mut variables = HashMap::new();
    variables.insert("$klist".to_string(), kmers.join(" "));

    let resp = client.new_readonly_txn().query_with_vars(query, variables);
    let r = match resp{
        Ok(resp) =>  resp,
        Err(resp) => return Err(Error::new(ErrorKind::Other, format!("Query failed {}", resp)))
    };
    let r_json: FindAll = serde_json::from_slice(&r.json)?;
    println!("{:?}", r_json.find_all);

    Ok(())
}
