use crate::genome;

use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use dgraph::{Dgraph, make_dgraph, Operation, Payload};

// New DB
pub fn create_dgraph_connection(addr: &str) -> Result<dgraph::Dgraph, Error>{
    let cx = make_dgraph!(dgraph::new_dgraph_client(addr));
    Ok(cx)
}

// Clean DB
pub fn drop_all(client: &Dgraph) -> Result<Payload, Error>{
    let op_drop = Operation{
      drop_all: true,
        ..Default::default()
    };

    let r = client.alter(&op_drop);
    match r{
        Ok(r) => Ok(r),
        Err(r) =>  Err(Error::new(ErrorKind::Other, "Could not drop all data from graph"))
    }
}



// Initial schema
pub fn set_schema(client: &Dgraph) -> Result<Payload, Error>{
    let op_schema = Operation{
        schema: r#"kmer: string @index(exact, term) ."#.to_string(),
        ..Default::default()
    };

    let r = client.alter(&op_schema);
    match r{
        Ok(r) => Ok(r),
        Err(r) =>  Err(Error::new(ErrorKind::Other, "Could not set dgraph schema"))
    }
}

