use std::io::{stdin, stdout, Write};

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use client_interface::client;
use client_interface::client::{read_from, write_to, Key, Value};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut client = "127.0.0.1:4241".to_string();
    let mut stream = TcpStream::connect(&client).await?;

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
                        println!("{}", client_turn);
                    } else {
                        client = split[1].to_string();
                        stream = TcpStream::connect(&client).await?;
                    }
                }
                "r" => {
                    match read(&mut stream, split[1]).await {
                        Ok(_) => {}
                        Err(err) => println!("{}", err),
                    };
                }
                "w" => {
                    match write(&mut stream, split[1], split[2]).await {
                        Ok(_) => {}
                        Err(err) => println!("{}", err),
                    };
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

async fn read(stream: &mut TcpStream, key: &str) -> std::io::Result<()> {
    let query = client::Query::read(Key(key.as_bytes().to_vec()));
    write_to(stream, &query).await?;
    stream.flush().await?;
    let result: client::Answer = read_from(stream).await?;

    println!("{:?}", result);
    Ok(())
}

async fn write(stream: &mut TcpStream, key: &str, value: &str) -> std::io::Result<()> {
    let query = client::Query::write(
        Key(key.as_bytes().to_vec()),
        Value(value.as_bytes().to_vec()),
    );
    write_to(stream, &query).await?;
    stream.flush().await?;

    let result: client::Answer = read_from(stream).await?;

    println!("{:?}", result);
    Ok(())
}
