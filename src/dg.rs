use crate::genome;
use dgraph;

use std::collections::HashMap;
use std::io::{Error, ErrorKind};


pub fn create_dgraph_connection(addr: &str) -> Result<dgraph::Dgraph, Error>{
    let cx = dgraph::make_dgraph!(dgraph::new_dgraph_client(addr));
    Ok(cx)
}

