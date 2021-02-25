use protobuf::Message;
use rand::Rng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use olympus::proto::queries;
use olympus::proto::queries::Answers;
use std::future::Future;
use std::time::SystemTime;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let clients = vec!["127.0.0.1:4241", "127.0.0.1:4242", "127.0.0.1:4243"];
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
    let mut stream = timed("read tcp connect", async {
        TcpStream::connect(client).await.unwrap()
    })
    .await;

    let buf = timed("read serialize", async {
        let mut read = queries::Read::new();
        read.set_key(key.as_bytes().to_vec());
        let mut command = queries::Commands::new();
        command.set_read(read);
        command.set_field_type(queries::Commands_CommandType::Read);
        command.write_to_bytes().unwrap()
    })
    .await;

    timed("read write socket", async {
        stream.write_u64(buf.len() as u64).await.unwrap();
        stream.write_all(&buf).await.unwrap();
        stream.flush().await.unwrap();
    })
    .await;

    let response_size = timed("read time to answer", async {
        stream.read_u64().await.unwrap()
    })
    .await;
    let mut buf = vec![0; response_size as usize];
    stream.read_exact(&mut buf).await?;

    let result = Answers::parse_from_bytes(&buf).unwrap();

    println!("{:?}", result);
    Ok(())
}

async fn write(client: &str, key: &str, value: &str) -> std::io::Result<()> {
    let mut stream = timed("write tcp connect", async {
        TcpStream::connect(client).await.unwrap()
    })
    .await;

    let buf = timed("write serialize", async {
        let mut write = queries::Write::new();
        write.set_key(key.as_bytes().to_vec());
        write.set_value(value.as_bytes().to_vec());
        let mut command = queries::Commands::new();
        command.set_write(write);
        command.set_field_type(queries::Commands_CommandType::Write);

        command.write_to_bytes().unwrap()
    })
    .await;

    timed("write socket", async {
        stream.write_u64(buf.len() as u64).await.unwrap();
        stream.write_all(&buf).await.unwrap();
        stream.flush().await.unwrap();
    })
    .await;

    let response_size = timed("write time to answer", async {
        stream.read_u64().await.unwrap()
    })
    .await;
    let mut buf = vec![0; response_size as usize];
    stream.read_exact(&mut buf).await?;

    let result = Answers::parse_from_bytes(&buf).unwrap();

    println!("write {} -> {} : {:?}", key, value, result);
    Ok(())
}

async fn timed<F>(label: &str, f: F) -> F::Output
where
    F: Future,
{
    let now = SystemTime::now();
    let t = f.await;
    println!("{}: {:?}", label, now.elapsed().unwrap());
    t
}
