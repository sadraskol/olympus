use std::io::{stdin, stdout, Write};

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use client_interface::client;
use client_interface::client::{write_to, read_from, Key, Value};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut client = "127.0.0.1:4241".to_string();
    loop {
        print!("olympus > ");
        stdout().flush().unwrap();

        let stdin = stdin();

        let mut buf = String::new();
        stdin.read_line(&mut buf)?;

        let split: Vec<&str> = buf.trim().split(' ').collect();
        if split.is_empty() {
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

    let query = client::Query::read(Key(key.as_bytes().to_vec()));
    write_to(&query, &mut stream).await?;
    stream.flush().await?;
    let result: client::Answer = read_from(&mut stream).await?;

    println!("{:?}", result);
    Ok(())
}

async fn write(client: &str, key: &str, value: &str) -> std::io::Result<()> {
    let mut stream = TcpStream::connect(client).await?;

    let query = client::Query::write(
        Key(key.as_bytes().to_vec()),
        Value(value.as_bytes().to_vec())
    );
    write_to(&query, &mut stream).await?;
    stream.flush().await?;

    let result: client::Answer = read_from(&mut stream).await?;

    println!("{:?}", result);
    Ok(())
}
