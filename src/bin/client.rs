use olympus::proto::queries;
use protobuf::Message;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:12345").await?;

    let mut read = queries::Read::new();
    read.set_key(vec![1, 2, 3]);
    let mut command = queries::Commands::new();
    command.set_read(read);
    command.set_field_type(queries::Commands_CommandType::Read);

    let buf = command.write_to_bytes()?;

    stream.write_u64(buf.len() as u64).await?;
    stream.write_all(&buf).await?;
    stream.flush().await?;
    let mut str = String::new();
    BufReader::new(&mut stream).read_line(&mut str).await?;

    println!("answer: {}", str);
    Ok(())
}
