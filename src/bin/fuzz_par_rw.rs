use std::future::Future;
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use protobuf::Message;
use rand::Rng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

use olympus::proto::queries;
use olympus::proto::queries::Answers;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let clients = Arc::new(["127.0.0.1:4241", "127.0.0.1:4242", "127.0.0.1:4243"]);
    let key_range = Arc::new(1..=5);
    let value_gen = Arc::new(AtomicI32::new(0));

    let first = spawn_rw_loop(clients.clone(), key_range.clone(), value_gen.clone());
    let second = spawn_rw_loop(clients.clone(), key_range.clone(), value_gen.clone());
    let third = spawn_rw_loop(clients.clone(), key_range.clone(), value_gen.clone());
    let last = spawn_rw_loop(clients, key_range, value_gen);

    first.await?;
    second.await?;
    third.await?;
    last.await?;

    Ok(())
}

fn spawn_rw_loop(
    clients: Arc<[&'static str]>,
    key_range: Arc<RangeInclusive<i32>>,
    value_gen: Arc<AtomicI32>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            random_rw(&clients, &key_range, &value_gen).await.unwrap();
        }
    })
}

async fn random_rw(
    clients: &[&str],
    key_range: &RangeInclusive<i32>,
    value_gen: &AtomicI32,
) -> std::io::Result<()> {
    let key = rand::thread_rng().gen_range(key_range.clone());
    let client = clients[rand::thread_rng().gen_range(0..=2)];
    if rand::random() {
        println!("loop reading {} from {}", key, client);
        read(client, &key.to_string()).await?;
    } else {
        println!(
            "loop writing {} -> {} to {}",
            key,
            value_gen.load(Ordering::SeqCst),
            client
        );
        let new_value = value_gen.fetch_add(1, Ordering::SeqCst);
        write(client, &key.to_string(), &new_value.to_string()).await?;
    }
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

async fn read(client: &str, key: &str) -> std::io::Result<Answers> {
    let mut stream = timed("tcp connect", async {
        TcpStream::connect(client).await.unwrap()
    })
    .await;

    let buf = timed("serialize", async {
        let mut read = queries::Read::new();
        read.set_key(key.as_bytes().to_vec());
        let mut command = queries::Commands::new();
        command.set_read(read);
        command.set_field_type(queries::Commands_CommandType::Read);
        command.write_to_bytes().unwrap()
    })
    .await;

    timed("write socket", async {
        stream.write_u64(buf.len() as u64).await.unwrap();
        stream.write_all(&buf).await.unwrap();
        stream.flush().await.unwrap();
    })
    .await;

    let response_size = timed("time to answer", async { stream.read_u64().await.unwrap() }).await;
    let mut buf = vec![0; response_size as usize];
    stream.read_exact(&mut buf).await?;

    let result = Answers::parse_from_bytes(&buf).unwrap();

    println!("{:?}", result);
    Ok(result)
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
