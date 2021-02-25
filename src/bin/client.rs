use std::io::{stdin, stdout, Write};

use protobuf::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use olympus::proto::queries;
use olympus::proto::queries::Answers;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut client = "127.0.0.1:4241".to_string();
    loop {
        print!("olympus > ");
        stdout().flush().unwrap();

        let stdin = stdin();

        let mut buf = String::new();
        stdin.read_line(&mut buf)?;

        let split: Vec<&str> = buf.trim().split(" ").collect();
        if split.len() < 1 {
            println!("malformed input");
        } else {
            let client_turn = client.clone();
            let word = split[0];
            match word {
                "c" => {
                    if split.len() == 1 {
                        println!("{}", client);
                    } else {
                        client = split[1].to_string();
                    }
                }
                "r" => {
                    read(&client_turn, split[1]).await?;
                }
                "w" => {
                    write(&client_turn, split[1], split[2]).await?;
                }
                "q" => {
                    return Ok(());
                }
                _ => {
                    println!("unknown command: {}", word);
                }
            }
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

    println!("{:?}", result);
    Ok(())
}
