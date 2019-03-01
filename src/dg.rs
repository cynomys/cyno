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
        // Giving an over-allocation of capacity prevent re-allocation later
        let mut all_quads = String::with_capacity(15000000);

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
//                create_batch_quads(&all_quads, &dkmers);
//                add_batch_dgraph(client, )


            }

            //            // Every 1000 kmers, add to dgraph
            //            if (i > 0) && (i % 30000 == 0){
            //                add_batch_dgraph(client,&all_quads)?;
            //                // Empty the string, but leave its capacity the same
            //                all_quads.clear();
            //            }
            //
            //            // For each pair of kmers, we need to do the following:
            //            // 1. Add kmer1
            //            // 2. Add kmer2
            //            // 3. Add an edge (genome name from schema) between them
            //            println!("There are {} kmers", all_kmers.len());
            //
            ////            for i in 0..(all_kmers.len() - 2) {
            //            for i in 0..50000{
            //                let kmer1 = format!(
            //                    "_:k{} <kmer> \"{}\" .\n",
            //                    all_kmers[i],
            //                    all_kmers[i]
            //                );
            //                all_quads.push_str(&kmer1);
            //
            //                let kmer2 = format!(
            //                    "_:k{} <kmer> \"{}\" .\n",
            //                    all_kmers[i+1],
            //                    all_kmers[i+1]
            //                );
            //                all_quads.push_str(&kmer2);
            //
            //                let kmer_edge = format!(
            //                    "_:k{} <{}> _:k{} .\n",
            //                    all_kmers[i],
            //                    k,
            //                    all_kmers[i+1]
            //                );
            //                all_quads.push_str(&kmer_edge);
            //
            //
            //            }
            //
            //            // Need to add any remaining quads
            //            if !all_quads.is_empty(){
            //                add_batch_dgraph(client, &all_quads)?;
            //            }
            break;
        }
    }

    Ok(())
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
