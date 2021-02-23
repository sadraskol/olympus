use olympus::proto::queries;
use protobuf::Message;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::net::TcpStream;
use olympus::proto::queries::Answers;
use olympus::config::cfg;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let config = cfg()?;
    let mut stream = TcpStream::connect(config.client_addr()).await?;

    let mut read = queries::Read::new();
    read.set_key(vec![1, 2, 3]);
    let mut command = queries::Commands::new();
    command.set_read(read);
    command.set_field_type(queries::Commands_CommandType::Read);

    let buf = command.write_to_bytes()?;

    stream.write_u64(buf.len() as u64).await?;
    stream.write_all(&buf).await?;
    stream.flush().await?;
    println!("flushed request");

    let response_size = stream.read_u64().await?;
    let mut buf = vec![0; response_size as usize];
    stream.read_exact(&mut buf).await?;

    let result = Answers::parse_from_bytes(&buf).unwrap();

    println!("answer: {:?}", result);
    Ok(())
}
