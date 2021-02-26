use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use protobuf::Message;
use rand::Rng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

use olympus::proto::queries;
use olympus::proto::queries::{Answers, WriteAnswer_WriteType};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let clients = Arc::new(["127.0.0.1:4241", "127.0.0.1:4242", "127.0.0.1:4243"]);
    let key_range = Arc::new(1..=5);
    let value_gen = Arc::new(AtomicI32::new(0));

    let first = spawn_rw_loop(1, clients.clone(), key_range.clone(), value_gen.clone());
    let second = spawn_rw_loop(2, clients.clone(), key_range.clone(), value_gen.clone());
    let third = spawn_rw_loop(3, clients.clone(), key_range.clone(), value_gen.clone());
    let fourth = spawn_rw_loop(4, clients.clone(), key_range.clone(), value_gen.clone());
    let last = spawn_rw_loop(5, clients, key_range, value_gen);

    first.await?;
    second.await?;
    third.await?;
    fourth.await?;
    last.await?;

    Ok(())
}

fn spawn_rw_loop(
    process_id: i32,
    clients: Arc<[&'static str]>,
    key_range: Arc<RangeInclusive<i32>>,
    value_gen: Arc<AtomicI32>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            random_rw(process_id, &clients, &key_range, &value_gen)
                .await
                .unwrap();
        }
    })
}

async fn random_rw(
    process_id: i32,
    clients: &[&str],
    key_range: &RangeInclusive<i32>,
    value_gen: &AtomicI32,
) -> std::io::Result<()> {
    let key = rand::thread_rng().gen_range(key_range.clone());
    let client = clients[rand::thread_rng().gen_range(0..=2)];
    if rand::random() {
        read(process_id, client, &key.to_string()).await?;
    } else {
        let new_value = value_gen.fetch_add(1, Ordering::SeqCst);
        write(process_id, client, &key.to_string(), &new_value.to_string()).await?;
    }
    Ok(())
}

async fn read(process_id: i32, client: &str, key: &str) -> std::io::Result<()> {
    let mut stream = TcpStream::connect(client).await.unwrap();

    let mut read = queries::Read::new();
    read.set_key(key.as_bytes().to_vec());
    let mut command = queries::Commands::new();
    command.set_read(read);
    command.set_field_type(queries::Commands_CommandType::Read);

    let buf = command.write_to_bytes().unwrap();

    stream.write_u64(buf.len() as u64).await.unwrap();
    stream.write_all(&buf).await.unwrap();
    stream.flush().await.unwrap();

    println!(
        "{{:type :invoke, :process {}, :value [:r {} nil]}}",
        process_id, key
    );

    let response_size = stream.read_u64().await.unwrap();
    let mut buf = vec![0; response_size as usize];
    stream.read_exact(&mut buf).await?;

    let result = Answers::parse_from_bytes(&buf).unwrap();

    if result.get_read().get_is_nil() {
        println!(
            "{{:type :ok, :process {}, :value [:r {} nil]}}",
            process_id, key
        );
    } else {
        println!(
            "{{:type :ok, :process {}, :value [:r {} {}]}}",
            process_id,
            key,
            std::str::from_utf8(result.get_read().get_value()).unwrap()
        );
    }
    Ok(())
}

async fn write(process_id: i32, client: &str, key: &str, value: &str) -> std::io::Result<()> {
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

    println!(
        "{{:type :invoke, :process {}, :value [:w {} {}]}}",
        process_id, key, value
    );

    let response_size = stream.read_u64().await?;
    let mut buf = vec![0; response_size as usize];
    stream.read_exact(&mut buf).await?;

    let result = Answers::parse_from_bytes(&buf).unwrap();

    match result.get_write().get_code() {
        WriteAnswer_WriteType::Ok => println!(
            "{{:type :ok, :process {}, :value [:w {} {}]}}",
            process_id, key, value
        ),
        WriteAnswer_WriteType::Refused => println!(
            "{{:type :fail, :process {}, :value [:w {} {}]}}",
            process_id, key, value
        ),
    }
    Ok(())
}
