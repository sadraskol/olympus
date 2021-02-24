use protobuf::Message;
use rand::Rng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use olympus::proto::queries;
use olympus::proto::queries::Answers;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let clients = vec![
        "127.0.0.1:4241",
        "127.0.0.1:4242",
        "127.0.0.1:4243",
    ];
    let key_range = 1..=5;
    let mut value_gen = 0;

    loop {
        let mut rng = rand::thread_rng();
        let key = rng.gen_range(key_range.clone());
        let client = clients[rng.gen_range(0..=2)];
        if rand::random() {
            println!("loop reading {} from {}", key, client);
            read(client, &key.to_string()).await?;
        } else {
            println!("loop writing {} -> {} from {}", key, value_gen, client);
            write(client, &key.to_string(), &value_gen.to_string()).await?;
            value_gen += 1;
        }
    }
}

async fn read(client: &str, key: &str) -> std::io::Result<()> {
    let mut stream = TcpStream::connect(client).await?;

    let mut read = queries::Read::new();
    read.set_key(key.as_bytes().to_vec());
    let mut command = queries::Commands::new();
    command.set_read(read);
    command.set_field_type(queries::Commands_CommandType::Read);

    let buf = command.write_to_bytes()?;

    stream.write_u64(buf.len() as u64).await?;
    stream.write_all(&buf).await?;
    stream.flush().await?;

    let response_size = stream.read_u64().await?;
    let mut buf = vec![0; response_size as usize];
    stream.read_exact(&mut buf).await?;

    let result = Answers::parse_from_bytes(&buf).unwrap();

    println!("{:?}", result);
    Ok(())
}

async fn write(client: &str, key: &str, value: &str) -> std::io::Result<()> {
    let mut stream = TcpStream::connect(client).await?;

    let mut write = queries::Write::new();
    write.set_key(key.as_bytes().to_vec());
    write.set_value(value.as_bytes().to_vec());
    let mut command = queries::Commands::new();
    command.set_write(write);
    command.set_field_type(queries::Commands_CommandType::Write);

    let buf = command.write_to_bytes()?;

    stream.write_u64(buf.len() as u64).await?;
    stream.write_all(&buf).await?;
    stream.flush().await?;

    let response_size = stream.read_u64().await?;
    let mut buf = vec![0; response_size as usize];
    stream.read_exact(&mut buf).await?;

    let result = Answers::parse_from_bytes(&buf).unwrap();

    println!("write {} -> {} : {:?}", key, value, result);
    Ok(())
}