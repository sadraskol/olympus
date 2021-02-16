use std::collections::HashSet;

mod proto;
mod state;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // let size = stream.read_u64().await?;
    // let mut buf = Vec::<u8>::with_capacity(size as usize);
    // buf.resize(size as usize, 0);
    // stream.read_exact(&mut buf).await?;
    Ok(())
}

/*
#[derive(Clone, Debug, Eq, PartialEq)]
enum Query {
    Write(Key, Value),
    Read(Key),
}

impl Query {
    fn from_bytes(bytes: &[u8]) -> std::io::Result<Query> {
        let c = Commands::parse_from_bytes(bytes)?;
        match c.get_field_type() {
            Commands_CommandType::Read => Ok(Query::Read(Key(c.get_read().get_key().to_vec()))),
            Commands_CommandType::Write => Ok(Query::Write(
                Key(c.get_write().get_key().to_vec()),
                Value(c.get_write().get_value().to_vec()),
            )),
        }
    }
}
*/