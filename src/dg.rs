use crate::genome;

use dgraph::{make_dgraph, Dgraph, Mutation, Operation, Payload};
use std::collections::HashMap;
use std::io::{Error, ErrorKind};


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
            "Could not drop all data from graph",
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

// Store kmers attached to nodes
// Store on a per-genome basis
pub fn add_genomes_dgraph(
    client: &Dgraph,
    hm: &HashMap<String, Vec<genome::ContigKmers>>,
) -> Result<(), Error> {
    // Iterate through all genomes
    for (k, v) in hm {
        // Vec of all data to insert
        // Giving an over-allocation of capacity prevent re-allocation later
        //
        let mut all_quads = String::with_capacity(15000000);

        // bulk_quads.append('_:{0} <kmer> "{0}" .{1}'.format(kmer, "\n"))
        // let test_quads = String::from("_:kTTTT <kmer> \"TTTT\" .\n");

        // Iterate through all contigs
        for contig in v {
            // Iterate through all kmers in the contig
            let all_kmers = contig.get_kmers_contig();

            // For each pair of kmers, we need to do the following:
            // 1. Add kmer1
            // 2. Add kmer2
            // 3. Add an edge (genome name from schema) between them
            println!("There are {} kmers", all_kmers.len());

//            for i in 0..(all_kmers.len() - 2) {
            for i in 0..50000{
                let kmer1 = format!(
                    "_:k{} <kmer> \"{}\" .\n",
                    all_kmers[i],
                    all_kmers[i]
                );
                all_quads.push_str(&kmer1);

                let kmer2 = format!(
                    "_:k{} <kmer> \"{}\" .\n",
                    all_kmers[i+1],
                    all_kmers[i+1]
                );
                all_quads.push_str(&kmer2);

                let kmer_edge = format!(
                    "_:k{} <{}> _:k{} .\n",
                    all_kmers[i],
                    k,
                    all_kmers[i+1]
                );
                all_quads.push_str(&kmer_edge);

                // Every 1000 kmers, add to dgraph
                if (i > 0) && (i % 30000 == 0){
                    add_batch_dgraph(client,&all_quads)?;
                    // Empty the string, but leave its capacity the same
                    all_quads.clear();
                }
            }

            // Need to add any remaining quads
            if !all_quads.is_empty(){
                add_batch_dgraph(client, &all_quads)?;
            }
        }
    }

    Ok(())
}

// Batch add the kmers,
fn add_batch_dgraph(client: &Dgraph, nq: &str)-> Result<(), Error>{
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