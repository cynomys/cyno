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
        schema: r#"kmer: string @index(exact, term) .
                   genomeA: uid .
                "#.to_string(),
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
        let mut all_quads = String::new();

        // bulk_quads.append('_:{0} <kmer> "{0}" .{1}'.format(kmer, "\n"))
        let test_quads = String::from("_:kTTTT <kmer> \"TTTT\" .\n");

        // Iterate through all contigs
        for contig in v {
            // Iterate through all kmers in the contig
            let all_kmers = contig.get_kmers_contig();
            for i in 0..(all_kmers.len() - 2) {
                let next_kmers = format!(
                    "{} {} {} .{}",
                    all_kmers[i],
                    "genomeA",
                    all_kmers[i + 1],
                    "\n",
                );
                all_quads.push_str(&next_kmers);
            }
            break;
        }
        // Insert one genome at a time
        let mut txn = client.new_txn();
        let mut mutation = Mutation::new();

        // Manual error propagation for now
        // The data is expected to be in u8 form for submission
        mutation.set_set_nquads(test_quads.into_bytes());

        println!("Mutation: {:?}", mutation);
        let m = txn.mutate(mutation);
        match m {
            Ok(m) => m,
            Err(..) => return Err(Error::new(ErrorKind::Other, "Failed to insert NQuads")),
        };

        // Commit
        let cc = txn.commit();
        match cc {
            Ok(..) => {},
            Err(..) => return Err(Error::new(ErrorKind::Other, "Transaction failed")),
        };
    }

    Ok(())
}
